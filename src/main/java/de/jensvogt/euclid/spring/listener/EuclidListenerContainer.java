package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.ees.model.Event;
import de.jensvogt.euclid.dto.eqs.model.Message;
import de.jensvogt.euclid.module.ees.EuclidEes;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls Euclid on behalf of {@code @QueueListener}- and {@code @BucketListener}-annotated methods
 * registered by {@code QueueListenerBeanPostProcessor} and {@code BucketListenerBeanPostProcessor},
 * dispatching what arrives to their handler and removing it afterward - deleting the message for a
 * queue listener, acknowledging the event for a bucket listener - unless that is turned off.
 *
 * <p>The two poll different things. A queue listener receives the messages of one EQS queue. A
 * bucket listener holds a durable EES subscription filtered to one bucket, so it accumulates only
 * the object events it asked for, keeps them across restarts and needs no queue to exist.
 *
 * <p>Starts one polling thread per registration when the Spring context is refreshed, and stops
 * them all on context close.
 */
public class EuclidListenerContainer implements SmartLifecycle {

    private static final Log logger = LogFactory.getLog(EuclidListenerContainer.class);

    private final EuclidEqs euclidSqs;
    private final EuclidEes euclidEes;
    private final EuclidEventStream eventStream;
    private final List<EuclidEventListener> eventListeners = new ArrayList<>();
    private final JsonMapper jsonMapper;
    private final ObjectMapper payloadMapper;
    private final List<QueueRegistration> queueRegistrations = new ArrayList<>();
    private final List<BucketRegistration> bucketRegistrations = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    /**
     * Builds a container that always asks for events rather than being told about them - the
     * behaviour before there was a connection to be told over, kept for a caller that has no
     * event stream to give it.
     */
    public EuclidListenerContainer(EuclidEqs euclidEqs, EuclidEes euclidEes,
                                   ObjectProvider<JsonMapper> objectMapperProvider) {
        this(euclidEqs, euclidEes, null, objectMapperProvider);
    }

    /**
     * @param eventStreamProvider the connection bucket listeners are told over, or {@code null} to
     *                            poll. A stream that cannot connect is not an error either: the
     *                            listener falls back to polling, which is what it did before.
     */
    public EuclidListenerContainer(EuclidEqs euclidEqs, EuclidEes euclidEes,
                                   ObjectProvider<EuclidEventStream> eventStreamProvider,
                                   ObjectProvider<JsonMapper> objectMapperProvider) {
        this.euclidSqs = euclidEqs;
        this.euclidEes = euclidEes;
        this.eventStream = eventStreamProvider == null ? null : eventStreamProvider.getIfAvailable();
        this.jsonMapper = objectMapperProvider.getIfAvailable(JsonMapper::new);
        // An event payload carries every field ESM publishes, so a handler taking a type of its
        // own names the few it cares about rather than all fifteen.
        this.payloadMapper = jsonMapper.rebuild().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    }

    public void register(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                          boolean autoDelete) {
        method.setAccessible(true);
        queueRegistrations.add(new QueueRegistration(bean, method, queueName, maxMessages, waitTime, autoDelete));
    }

    /**
     * Registers a handler for the object events of {@code bucketName}, claimed under the durable
     * subscriber name {@code subscriber}.
     */
    public void registerBucket(Object bean, Method method, String subscriber, String bucketName, String prefix,
                               boolean directories, List<String> eventTypes, long maxEvents, long waitTime,
                               long visibilityTimeout, boolean autoAck) {
        method.setAccessible(true);
        bucketRegistrations.add(new BucketRegistration(bean, method, subscriber, bucketName, prefix, directories,
                eventTypes, maxEvents, waitTime, visibilityTimeout, autoAck));
    }

    @Override
    public void start() {
        int listeners = queueRegistrations.size() + bucketRegistrations.size();
        if (listeners == 0 || !running.compareAndSet(false, true)) {
            return;
        }
        executor = Executors.newFixedThreadPool(listeners, runnable -> {
            Thread thread = new Thread(runnable, "euclid-listener");
            thread.setDaemon(false);
            return thread;
        });
        for (QueueRegistration registration : queueRegistrations) {
            executor.submit(() -> pollQueue(registration));
        }
        for (BucketRegistration registration : bucketRegistrations) {
            if (startListening(registration)) {
                continue;
            }
            executor.submit(() -> pollEvents(registration));
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
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

    private void pollQueue(QueueRegistration registration) {
        String ern;
        try {
            ern = euclidSqs.getQueueErn(registration.queueName()).ern();
        } catch (Exception e) {
            logger.error("Could not resolve queue '" + registration.queueName() + "', listener not started", e);
            return;
        }

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
                logger.error("Error polling queue '" + registration.queueName() + "'", e);
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
     * @return whether the listener was started; {@code false} means the caller should poll.
     */
    private boolean startListening(BucketRegistration registration) {
        if (eventStream == null || !registration.autoAck()) {
            return false;
        }
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
                    .build();
            listener.start();
            eventListeners.add(listener);
            return true;
        } catch (Exception e) {
            // Every reason to end up here - websockets disabled on the gateway, a proxy in the
            // way, an older server - is a reason to go back to asking rather than to fail the
            // application's startup.
            logger.warn("Could not listen for '" + registration.bucketName()
                    + "' over the event stream, falling back to polling", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    private void pollEvents(BucketRegistration registration) {
        try {
            euclidEes.subscribeEvents(registration.subscriber(), registration.eventTypes(), registration.filter());
        } catch (Exception e) {
            logger.error("Could not subscribe '" + registration.subscriber() + "' to bucket '"
                    + registration.bucketName() + "', listener not started", e);
            return;
        }

        while (running.get() && !Thread.currentThread().isInterrupted()) {
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

    private void dispatch(QueueRegistration registration, Message message) {
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
            logger.error("Listener for queue '" + registration.queueName() + "' failed to handle message "
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

    private record QueueRegistration(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                                     boolean autoDelete) {
    }

    private record BucketRegistration(Object bean, Method method, String subscriber, String bucketName, String prefix,
                                      boolean directories, List<String> eventTypes, long maxEvents, long waitTime,
                                      long visibilityTimeout, boolean autoAck) {

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
