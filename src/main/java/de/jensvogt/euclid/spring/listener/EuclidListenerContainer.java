package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.ees.model.Event;
import de.jensvogt.euclid.dto.ens.model.Subscription;
import de.jensvogt.euclid.dto.eqs.model.Message;
import de.jensvogt.euclid.exception.EuclidServiceException;
import de.jensvogt.euclid.module.ees.EuclidEes;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import de.jensvogt.euclid.ws.EuclidEventListener;
import de.jensvogt.euclid.ws.EuclidEventStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.StringUtils;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls Euclid on behalf of {@code @QueueListener}- and {@code @BucketListener}-annotated methods
 * registered by {@code QueueListenerBeanPostProcessor} and {@code BucketListenerBeanPostProcessor},
 * dispatching what arrives to their handler and removing it afterward - deleting the message for a
 * queue listener, acknowledging the event for a bucket listener - unless that is turned off.
 *
 * <p>They poll different things. A queue listener receives the messages of one EQS queue. A topic
 * listener receives from the queue it subscribes to an ENS topic, since fanning out to queues is
 * the only delivery a topic has. A bucket listener holds a durable EES subscription filtered to one
 * bucket, so it accumulates only the object events it asked for, keeps them across restarts and
 * needs no queue to exist.
 *
 * <p>Starts one polling thread per registration when the Spring context is refreshed, and stops
 * them all on context close.
 */
public class EuclidListenerContainer implements SmartLifecycle {

    private static final Log logger = LogFactory.getLog(EuclidListenerContainer.class);

    /**
     * How often to try the event stream again for a bucket that fell back to polling.
     *
     * <p>Long enough that a gateway which genuinely has no websocket support is not asked
     * constantly, short enough that an application which lost the race against its own
     * installation starting up is back on the stream within a minute of it being available.
     */
    private static final long STREAM_RETRY_SECONDS = 30;

    private final ObjectProvider<EuclidEqs> euclidSqsProvider;
    private final ObjectProvider<EuclidEes> euclidEesProvider;
    private final ObjectProvider<EuclidEns> euclidEnsProvider;
    private final ObjectProvider<EuclidEventStream> eventStreamProvider;
    private final ObjectProvider<JsonMapper> objectMapperProvider;

    private EuclidEqs euclidSqs;
    private EuclidEes euclidEes;
    private EuclidEns euclidEns;
    // Read by stop(), which a shutdown hook can call on a thread other than the one that started.
    private volatile EuclidEventStream eventStream;
    private JsonMapper jsonMapper;
    private ObjectMapper payloadMapper;
    // Appended to from the scheduler thread when a retry gets the stream working, and read by
    // stop() from whichever thread shuts the context down.
    private final List<EuclidEventListener> eventListeners = new CopyOnWriteArrayList<>();
    private final List<QueueRegistration> queueRegistrations = new ArrayList<>();
    private final List<TopicRegistration> topicRegistrations = new ArrayList<>();
    private final List<BucketRegistration> bucketRegistrations = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    /**
     * Drives the periodic claim that backs up the pushed listeners; created only if there is a
     * pushed listener to back up.
     */
    private ScheduledExecutorService safetyNet;

    /**
     * Whether the last startListening() attempt was refused outright rather than failing for a
     * reason that might not hold next time - see isRefusal(). Only ever written and read on the
     * one thread making an attempt for a given registration (start(), then that registration's
     * retry task), so it needs no synchronisation of its own.
     */
    private volatile boolean lastAttemptWasRefused;

    /**
     * Builds a container that always asks for events rather than being told about them - the
     * behaviour before there was a connection to be told over, kept for a caller that has no
     * event stream to give it.
     */
    public EuclidListenerContainer(ObjectProvider<EuclidEqs> euclidSqsProvider,
                                   ObjectProvider<EuclidEes> euclidEesProvider,
                                   ObjectProvider<EuclidEns> euclidEnsProvider,
                                   ObjectProvider<JsonMapper> objectMapperProvider) {
        this(euclidSqsProvider, euclidEesProvider, euclidEnsProvider, null, objectMapperProvider);
    }

    /**
     * Takes every client as a provider and resolves none of them here, so that constructing the
     * container - which the bean post processors do while the context is still starting up - logs
     * nobody in and opens nothing. {@link #start()} resolves what the registered listeners
     * actually need, once the context is refreshed.
     *
     * @param eventStreamProvider the connection bucket listeners are told over, or {@code null} to
     *                            poll. A stream that cannot connect is not an error either: the
     *                            listener falls back to polling, which is what it did before.
     */
    public EuclidListenerContainer(ObjectProvider<EuclidEqs> euclidSqsProvider,
                                   ObjectProvider<EuclidEes> euclidEesProvider,
                                   ObjectProvider<EuclidEns> euclidEnsProvider,
                                   ObjectProvider<EuclidEventStream> eventStreamProvider,
                                   ObjectProvider<JsonMapper> objectMapperProvider) {
        this.euclidSqsProvider = euclidSqsProvider;
        this.euclidEesProvider = euclidEesProvider;
        this.euclidEnsProvider = euclidEnsProvider;
        this.eventStreamProvider = eventStreamProvider;
        this.objectMapperProvider = objectMapperProvider;
    }

    public void register(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                          boolean autoDelete, int concurrency) {
        method.setAccessible(true);
        queueRegistrations.add(new QueueRegistration(bean, method, queueName, maxMessages, waitTime, autoDelete,
                concurrency));
    }

    /**
     * Registers a handler for the messages published to {@code topicName}, delivered through
     * {@code queueName} - the queue this listener subscribes to the topic.
     */
    public void registerTopic(Object bean, Method method, String topicName, String queueName, long maxMessages,
                              long waitTime, boolean autoDelete, int concurrency) {
        method.setAccessible(true);
        topicRegistrations.add(new TopicRegistration(bean, method, topicName, queueName, maxMessages, waitTime,
                autoDelete, concurrency));
    }

    /**
     * Registers a handler for the object events of {@code bucketName}, claimed under the durable
     * subscriber name {@code subscriber}.
     */
    public void registerBucket(Object bean, Method method, String subscriber, String bucketName, String prefix,
                               boolean directories, List<String> eventTypes, long maxEvents, long waitTime,
                               long visibilityTimeout, boolean autoAck, int concurrency) {
        method.setAccessible(true);
        bucketRegistrations.add(new BucketRegistration(bean, method, subscriber, bucketName, prefix, directories,
                eventTypes, maxEvents, waitTime, visibilityTimeout, autoAck, concurrency));
    }

    @Override
    public void start() {
        int listeners = concurrencySum(queueRegistrations) + concurrencySum(topicRegistrations)
                + concurrencySum(bucketRegistrations);
        if (listeners == 0 || !running.compareAndSet(false, true)) {
            return;
        }
        resolveClients();
        executor = Executors.newFixedThreadPool(listeners, runnable -> {
            Thread thread = new Thread(runnable, "euclid-listener");
            thread.setDaemon(false);
            return thread;
        });
        for (QueueRegistration registration : queueRegistrations) {
            startQueueListener(registration);
        }
        for (TopicRegistration registration : topicRegistrations) {
            startTopicListener(registration);
        }
        for (BucketRegistration registration : bucketRegistrations) {
            startBucketListener(registration);
        }
    }

    /**
     * How many threads the executor must hold for one kind of registration: each registration's
     * own {@code concurrency}, floored at one so a stray non-positive value cannot leave a listener
     * with no thread at all.
     */
    private static int concurrencySum(List<? extends HasConcurrency> registrations) {
        return registrations.stream().mapToInt(registration -> Math.max(1, registration.concurrency())).sum();
    }

    /**
     * Listens for a bucket's events over the event stream, or by asking until it can.
     *
     * <p>The stream is the better of the two and is tried first. When it cannot be had, polling
     * starts immediately so nothing is missed, and the stream is tried again periodically until it
     * works - at which point polling stops and the listener switches over.
     *
     * <p>That retry is the point. The likeliest reason the first attempt fails is that the
     * application came up while euclid was still coming up and its subscribe timed out against a
     * module not yet answering. That condition lasts seconds; without a retry it decided how the
     * application ran for the rest of its life, with one WARN at start-up as the only trace.
     */
    private void startBucketListener(BucketRegistration registration) {
        EuclidEventListener listener = startListening(registration);
        if (listener != null) {
            scheduleSafetyNet(registration, listener);
            return;
        }

        // Refused, not unavailable: subscribing is not allowed for this caller, so neither the
        // stream nor polling will ever work and both would only repeat the rejection.
        if (lastAttemptWasRefused) {
            return;
        }

        AtomicBoolean streaming = new AtomicBoolean(false);
        for (int i = 0; i < Math.max(1, registration.concurrency()); i++) {
            executor.submit(() -> pollEvents(registration, streaming));
        }

        // Only worth retrying something that could have worked. A stream this application will
        // never have - no gateway websocket, or a registration that acknowledges its own events -
        // is not a transient failure, and asking again every half minute forever would say so in
        // the log forever.
        if (eventStream != null && registration.autoAck()) {
            scheduleStreamRetry(registration, streaming);
        }
    }

    /**
     * Tries the event stream again every so often, until it works or the container stops.
     *
     * <p>While this is retrying, the bucket is being polled, so the two can briefly both be
     * delivering: the poller may be inside a long receive-events call at the moment the stream
     * takes over. Nothing is processed twice because of it - the claim in receive-events decides
     * who handles an event, the same mechanism that makes the safety net safe - and the poller
     * stops at the end of that call.
     */
    private void scheduleStreamRetry(BucketRegistration registration, AtomicBoolean streaming) {
        ensureScheduler();

        // Held in a one-element array so the task can cancel itself once it has succeeded:
        // scheduleWithFixedDelay only hands the handle back after the task already exists.
        final ScheduledFuture<?>[] retry = new ScheduledFuture<?>[1];
        retry[0] = safetyNet.scheduleWithFixedDelay(() -> {
            if (!running.get() || streaming.get()) {
                if (retry[0] != null) retry[0].cancel(false);
                return;
            }
            EuclidEventListener listener = startListening(registration);
            if (listener == null) {
                return;
            }
            logger.info("Listening for '" + registration.bucketName() + "' over the event stream again, polling stopped");
            // Set before the safety net is scheduled, so the poller is already on its way out
            // before anything else starts claiming for this registration.
            streaming.set(true);
            scheduleSafetyNet(registration, listener);
            if (retry[0] != null) retry[0].cancel(false);
        }, STREAM_RETRY_SECONDS, STREAM_RETRY_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Asks a pushed listener to look for itself every so often, on top of being told.
     * <p>
     * A push is meant to be an optimisation - the events are in the store either way, and the
     * server's own comment for it says a push that fails costs latency because "the subscriber
     * finds the event on its next receive-events". A listener driven only by pushes has no next
     * receive-events, so anything that stops them reaching it - a connection replaced without its
     * subscriptions, a gateway that forgot the session, a dropped frame - turns into silence that
     * lasts until the application is restarted, with a growing backlog and nothing logged. This is
     * what makes that a delay of at most one interval instead.
     * <p>
     * It costs one claim per interval per listener when there is nothing waiting, and it cannot
     * double-process: the drain it triggers is the listener's own, on the listener's own thread,
     * and the claim in receive-events is what decides who handles an event.
     */
    private void scheduleSafetyNet(BucketRegistration registration, EuclidEventListener listener) {
        ensureScheduler();
        long interval = Math.max(registration.waitTime(), 1);
        safetyNet.scheduleWithFixedDelay(() -> {
            try {
                listener.onNotify(registration.bucketName());
            } catch (RuntimeException e) {
                logger.debug("Safety-net claim for bucket '" + registration.bucketName() + "' failed", e);
            }
        }, interval, interval, TimeUnit.SECONDS);
    }

    /**
     * Creates the shared scheduler on first use, for the safety net and the stream retry.
     *
     * <p>One thread for both: neither does more than start a claim or attempt a subscribe, and a
     * container with no bucket listeners at all should not be holding a thread it never uses.
     */
    private void ensureScheduler() {
        if (safetyNet == null) {
            safetyNet = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "euclid-listener-safety-net");
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (safetyNet != null) {
            safetyNet.shutdownNow();
            safetyNet = null;
        }
        for (EuclidEventListener listener : eventListeners) {
            listener.close();
        }
        eventListeners.clear();
        if (eventStream != null) {
            eventStream.close();
        }
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Resolves the clients the registered listeners need, and only those: an application whose
     * listeners are all {@code @QueueListener} never asks for an ENS or EES client, so it never
     * needs those beans to exist. Called from {@link #start()} after the early return, so a
     * context that registered no listener at all resolves nothing.
     */
    private void resolveClients() {
        if (!queueRegistrations.isEmpty() || !topicRegistrations.isEmpty()) {
            euclidSqs = euclidSqsProvider.getObject();
        }
        if (!topicRegistrations.isEmpty()) {
            euclidEns = euclidEnsProvider.getObject();
        }
        if (!bucketRegistrations.isEmpty()) {
            euclidEes = euclidEesProvider.getObject();
            eventStream = eventStreamProvider == null ? null : eventStreamProvider.getIfAvailable();
        }
        jsonMapper = objectMapperProvider.getIfAvailable(JsonMapper::new);
        // An event payload carries every field ESM publishes, so a handler taking a type of its
        // own names the few it cares about rather than all fifteen.
        payloadMapper = jsonMapper.rebuild().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    }

    /**
     * Resolves the queue once, then hands the same ERN to {@link QueueRegistration#concurrency()}
     * independent polling threads - resolving once rather than per thread is what lets concurrency
     * be more than one without asking the same question that many times over.
     */
    private void startQueueListener(QueueRegistration registration) {
        String ern;
        try {
            ern = euclidSqs.getQueueErn(registration.queueName()).ern();
        } catch (Exception e) {
            logger.error("Could not resolve " + registration.describe() + ", listener not started", e);
            return;
        }
        for (int i = 0; i < Math.max(1, registration.concurrency()); i++) {
            executor.submit(() -> pollMessages(registration, ern));
        }
    }

    /**
     * Resolves - subscribing if not already - the delivery queue once, then hands the same ERN to
     * {@link TopicRegistration#concurrency()} independent polling threads. Resolving once rather
     * than per thread is also what keeps a first-time subscribe from being attempted more than
     * once.
     */
    private void startTopicListener(TopicRegistration registration) {
        String ern;
        try {
            ern = subscribedQueueErn(registration);
        } catch (Exception e) {
            logger.error("Could not subscribe to " + registration.describe() + ", listener not started", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return;
        }
        for (int i = 0; i < Math.max(1, registration.concurrency()); i++) {
            executor.submit(() -> pollMessages(registration, ern));
        }
    }

    /**
     * Resolves the queue a topic listener receives from, creating the queue and the subscription
     * delivering into it if they don't exist yet. A topic fans out to its subscribed queues and
     * has no other delivery, so this is what "listening to a topic" is.
     */
    private String subscribedQueueErn(TopicRegistration registration) throws Exception {
        String topicErn = euclidEns.getTopicErn(registration.topicName()).ern();
        String queueErn = queueErn(registration.queueName());

        List<Subscription> subscriptions = euclidEns.listSubscriptions(topicErn).subscriptions();
        boolean subscribed = subscriptions != null && subscriptions.stream()
                .anyMatch(subscription -> queueErn.equals(subscription.targetErn()));
        if (!subscribed) {
            euclidEns.subscribe(topicErn, queueErn);
            logger.info("Subscribed queue '" + registration.queueName() + "' to topic '"
                    + registration.topicName() + "'");
        }
        return queueErn;
    }

    /**
     * The ERN of {@code queueName}, creating the queue if the server has none by that name - a
     * listener naming the queue its topic is delivered to should not also have to create it.
     */
    private String queueErn(String queueName) throws Exception {
        try {
            String ern = euclidSqs.getQueueErn(queueName).ern();
            if (StringUtils.hasText(ern)) {
                return ern;
            }
        } catch (RuntimeException e) {
            logger.debug("Queue '" + queueName + "' could not be resolved, creating it", e);
        }
        return euclidSqs.createQueue(queueName).ern();
    }

    private void pollMessages(MessageRegistration registration, String ern) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<Message> messages = euclidSqs
                        .receiveMessages(ern, registration.maxMessages(), registration.waitTime())
                        .messages();
                for (Message message : messages) {
                    dispatch(registration, message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error polling " + registration.describe(), e);
            }
        }
    }

    /**
     * Starts a listener that is told when its bucket changes, rather than one that asks.
     *
     * <p>Two cases still ask. Without an event stream there is nothing to be told over. And a
     * registration with {@code autoAck = false} means the handler acknowledges events itself,
     * which the pushed listener cannot honour - it acknowledges what the handler returns from -
     * so honouring the annotation matters more than the connection.
     *
     * @return the listener that was started, or {@code null} if the caller should poll instead.
     */
    private EuclidEventListener startListening(BucketRegistration registration) {
        if (eventStream == null || !registration.autoAck()) {
            return null;
        }
        lastAttemptWasRefused = false;
        try {
            EuclidEventListener listener = EuclidEventListener.builder()
                    .ees(euclidEes)
                    .stream(eventStream)
                    .name(registration.subscriber())
                    .eventTypes(registration.eventTypes())
                    .filter(registration.filter())
                    // Acknowledged by the listener when the handler returns, so dispatch() must
                    // not swallow the failure the way the polling path does - throwing is what
                    // leaves the event to be delivered again.
                    .handler(event -> dispatchOrThrow(registration, event))
                    .concurrency(registration.concurrency())
                    .build();
            listener.start();
            eventListeners.add(listener);
            return listener;
        } catch (Exception e) {
            // Every reason to end up here - websockets disabled on the gateway, a proxy in the
            // way, an older server - is a reason to go back to asking rather than to fail the
            // application's startup.
            lastAttemptWasRefused = isRefusal(e);
            logger.warn("Could not listen for '" + registration.bucketName()
                    + "' over the event stream, falling back to polling", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    /**
     * Whether a failure means "not allowed" rather than "not right now".
     *
     * <p>The difference decides whether anything is worth trying again. A timeout, a refused
     * connection or a 5xx is a moment in time - most often this application starting before the
     * installation it talks to has finished starting - and asking again is exactly right. Being
     * told 401 or 403 is an answer about the caller, not about the moment: retrying it produces
     * the same answer indefinitely, and polling after it produces a loop of receive-events calls
     * that cannot work either.
     */
    private static boolean isRefusal(Exception e) {
        return e instanceof EuclidServiceException failure
               && (failure.statusCode() == 401 || failure.statusCode() == 403);
    }

    /**
     * Asks for a bucket's events in a loop, until the container stops or the event stream takes
     * over.
     *
     * @param registration what to ask for
     * @param streaming set once a retry has got the event stream working, which is this loop's
     *                  cue to stop - it finishes the receive it is in and exits
     */
    private void pollEvents(BucketRegistration registration, AtomicBoolean streaming) {
        try {
            euclidEes.subscribeEvents(registration.subscriber(), registration.eventTypes(), registration.filter());
        } catch (Exception e) {
            if (isRefusal(e)) {
                logger.error("Could not subscribe '" + registration.subscriber() + "' to bucket '"
                        + registration.bucketName() + "', listener not started", e);
                return;
            }
            // Anything else is a moment rather than an answer - most often this application
            // starting before the installation it subscribes to has finished starting. Polling
            // begins regardless: receive-events carries the subscription request the server needs,
            // and the stream retry scheduled alongside this loop subscribes again anyway.
            logger.warn("Could not subscribe '" + registration.subscriber() + "' to bucket '"
                    + registration.bucketName() + "' yet, polling and retrying", e);
        }

        while (running.get() && !streaming.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<Event> events = euclidEes.receiveEvents(registration.subscriber(), registration.maxEvents(),
                        registration.waitTime(), registration.visibilityTimeout()).events();
                for (Event event : events) {
                    dispatch(registration, event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.error("Error receiving events for bucket '" + registration.bucketName() + "'", e);
            }
        }
    }

    private void dispatch(MessageRegistration registration, Message message) {
        try {
            Method method = registration.method();
            Class<?> paramType = method.getParameterTypes()[0];
            Object arg;
            if (paramType == String.class) {
                arg = message.body();
            } else if (paramType.isAssignableFrom(Message.class)) {
                arg = message;
            } else {
                arg = jsonMapper.readValue(message.body(), paramType);
            }
            method.invoke(registration.bean(), arg);
            if (registration.autoDelete()) {
                euclidSqs.deleteMessage(message.receiptHandle());
            }
        } catch (Exception e) {
            logger.error("Listener for " + registration.describe() + " failed to handle message "
                    + message.ern(), e);
        }
    }

    private void dispatch(BucketRegistration registration, Event event) {
        try {
            Method method = registration.method();
            Class<?> paramType = method.getParameterTypes()[0];
            Object arg;
            if (paramType.isAssignableFrom(Event.class)) {
                arg = event;
            } else if (paramType.isAssignableFrom(Map.class)) {
                arg = event.payload();
            } else {
                arg = payloadMapper.convertValue(event.payload(), paramType);
            }
            method.invoke(registration.bean(), arg);
            if (registration.autoAck()) {
                euclidEes.ackEvent(registration.subscriber(), event.eventId());
            }
        } catch (Exception e) {
            logger.error("Listener for bucket '" + registration.bucketName() + "' failed to handle event "
                    + event.eventId(), e);
        }
    }

    /**
     * Hands one event to a handler, letting a failure out so the listener leaves the event
     * unacknowledged - the polling path logs and moves on instead, because there it has already
     * been claimed and acknowledging is a separate step.
     */
    private void dispatchOrThrow(BucketRegistration registration, Event event) {
        try {
            Method method = registration.method();
            Class<?> paramType = method.getParameterTypes()[0];
            Object arg;
            if (paramType.isAssignableFrom(Event.class)) {
                arg = event;
            } else if (paramType.isAssignableFrom(Map.class)) {
                arg = event.payload();
            } else {
                arg = payloadMapper.convertValue(event.payload(), paramType);
            }
            method.invoke(registration.bean(), arg);
        } catch (Exception e) {
            logger.error("Listener for bucket '" + registration.bucketName() + "' failed to handle event "
                    + event.eventId(), e);
            throw new IllegalStateException("bucket listener failed to handle event " + event.eventId(), e);
        }
    }

    /**
     * What every kind of registration has in common: how many independent polling threads it
     * wants, so the executor can be sized without caring which kind of listener it is sizing for.
     */
    private interface HasConcurrency {

        int concurrency();
    }

    /**
     * What the two message-driven listeners have in common: both receive from a queue and differ
     * only in how that queue is arrived at - named outright, or subscribed to a topic.
     */
    private sealed interface MessageRegistration extends HasConcurrency permits QueueRegistration, TopicRegistration {

        Object bean();

        Method method();

        long maxMessages();

        long waitTime();

        boolean autoDelete();

        /**
         * How this listener is named in a log line, from the caller's point of view rather than
         * the queue's - a topic listener that says "queue" leaves the reader looking for a queue
         * nobody wrote down.
         */
        String describe();
    }

    private record QueueRegistration(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                                     boolean autoDelete, int concurrency) implements MessageRegistration {

        @Override
        public String describe() {
            return "queue '" + queueName + "'";
        }
    }

    private record TopicRegistration(Object bean, Method method, String topicName, String queueName, long maxMessages,
                                     long waitTime, boolean autoDelete, int concurrency) implements MessageRegistration {

        @Override
        public String describe() {
            return "topic '" + topicName + "' (queue '" + queueName + "')";
        }
    }

    private record BucketRegistration(Object bean, Method method, String subscriber, String bucketName, String prefix,
                                      boolean directories, List<String> eventTypes, long maxEvents, long waitTime,
                                      long visibilityTimeout, boolean autoAck, int concurrency)
            implements HasConcurrency {

        /**
         * The subscription filter, matched against an event payload at publish time. Only the
         * fields that narrow anything are sent: filtering on a directory marker's own key would
         * exclude the markers a listener asking for them wants.
         */
        private Map<String, Object> filter() {
            Map<String, Object> filter = new LinkedHashMap<>();
            filter.put("bucketName", bucketName);
            if (StringUtils.hasText(prefix)) {
                filter.put("prefix", prefix);
            }
            if (!directories) {
                filter.put("directory", false);
            }
            return filter;
        }
    }
}
