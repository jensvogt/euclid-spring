package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.ees.model.Event;
import de.jensvogt.euclid.dto.ens.model.Subscription;
import de.jensvogt.euclid.dto.eqs.model.Message;
import de.jensvogt.euclid.exception.EuclidServiceException;
import de.jensvogt.euclid.dto.com.Variant;
import de.jensvogt.euclid.dto.eqs.model.Queue;
import de.jensvogt.euclid.dto.emo.Metric;
import de.jensvogt.euclid.module.emo.EuclidEmo;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import de.jensvogt.euclid.module.esm.EuclidEsm;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.StringUtils;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Polls Euclid on behalf of {@code @QueueListener}-, {@code @TopicListener}- and
 * {@code @BucketListener}-annotated methods registered by the matching bean post processors,
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

    /** Message attribute the event type is read from, when the server sets one. */
    private static final String EVENT_TYPE_ATTRIBUTE = "eventType";

    /** Tag a bucket listener stamps on its queue to say the run that owns it is still alive. */
    private static final String HEARTBEAT_TAG = "euclid.listener.heartbeat";

    /** How often that tag is refreshed. */
    private static final long HEARTBEAT_SECONDS = 60;

    /**
     * How long a queue may go without a heartbeat before a sweep treats it as abandoned. Several
     * missed beats rather than one, so a slow tag write or a paused process is not mistaken for a
     * dead one.
     */
    private static final long HEARTBEAT_STALE_SECONDS = 300;

    /** How many queues one sweep looks at - far more than an application has listeners. */
    private static final long SWEEP_PAGE_SIZE = 200;

    /** Queue settings a delivery queue is created with, beyond its visibility timeout. */
    private static final long DEFAULT_MAX_RETRIES = 3;
    private static final long DEFAULT_MAX_MESSAGE_LENGTH = 1024 * 1024;


    private final ObjectProvider<EuclidEqs> euclidSqsProvider;
    private final ObjectProvider<EuclidEsm> euclidEsmProvider;
    private final ObjectProvider<EuclidEns> euclidEnsProvider;
    private final ObjectProvider<JsonMapper> objectMapperProvider;

    private final ObjectProvider<EuclidEmo> euclidEmoProvider;

    /**
     * Nanoseconds this process has spent inside listener handlers since the last report.
     *
     * <p>Summed across every listener thread, so it exceeds wall-clock time whenever more than one
     * handler runs at once - which is the point: it is measured against the concurrency the
     * listeners were sized for, not against the clock.
     */
    private final AtomicLong busyNanos = new AtomicLong();

    /**
     * Whether the last attempt to report load failed, so the first failure of a run can be logged
     * loudly and the ones after it quietly.
     */
    private final AtomicBoolean loadReportFailed = new AtomicBoolean(false);

    /**
     * Every queue this process is polling, however its listener arrived at one - named outright,
     * subscribed to a topic, or created for a bucket's object events. Recorded where they are all
     * the same thing (a queue being polled) rather than per listener type, so a listener kind
     * added later is covered without anyone remembering to.
     */
    private final Set<String> polledQueueErns = ConcurrentHashMap.newKeySet();

    /**
     * When the current measurement window began, so utilisation is a fraction of elapsed capacity
     * rather than of an assumed interval.
     */
    private final AtomicLong windowStartNanos = new AtomicLong(System.nanoTime());

    private EuclidEqs euclidSqs;

    /**
     * The same EQS client, marked as internal traffic, for the calls this container makes about
     * the system rather than on behalf of a listener: reading a queue's depth to report it, and
     * keeping a bucket delivery queue's heartbeat fresh. Both poll every few seconds, and counted
     * as load they would hold EQS awake for as long as this application runs.
     */
    private EuclidEqs euclidSqsInternal;
    private EuclidEsm euclidEsm;
    private EuclidEns euclidEns;
    private JsonMapper jsonMapper;
    private ObjectMapper payloadMapper;

    /**
     * The queue and subscription each bucket listener created, so stop() can take them down again.
     * Written from the thread that prepared the listener and read by whichever thread shuts the
     * context down, hence a concurrent map.
     */
    private final Map<String, BucketDelivery> bucketDeliveries = new ConcurrentHashMap<>();

    private final List<QueueRegistration> queueRegistrations = new ArrayList<>();
    private final List<TopicRegistration> topicRegistrations = new ArrayList<>();
    private final List<BucketRegistration> bucketRegistrations = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    /**
     * Refreshes the heartbeat tag of every bucket listener's queue; created only if there is a
     * bucket listener to keep alive.
     */
    private ScheduledExecutorService heartbeats;

    /**
     * Identifies this run, so the queue a bucket listener creates belongs to it alone - which is
     * what makes deleting that queue on shutdown safe, and what lets another run recognise the
     * queues a crashed one left behind.
     */
    private final String runId = UUID.randomUUID().toString().substring(0, 8);

    /**
     * Takes every client as a provider and resolves none of them here, so that constructing the
     * container - which the bean post processors do while the context is still starting up - logs
     * nobody in and opens nothing. {@link #start()} resolves what the registered listeners
     * actually need, once the context is refreshed.
     */
    public EuclidListenerContainer(ObjectProvider<EuclidEqs> euclidSqsProvider,
                                   ObjectProvider<EuclidEsm> euclidEsmProvider,
                                   ObjectProvider<EuclidEns> euclidEnsProvider,
                                   ObjectProvider<EuclidEmo> euclidEmoProvider,
                                   ObjectProvider<JsonMapper> objectMapperProvider) {
        this.euclidSqsProvider = euclidSqsProvider;
        this.euclidEsmProvider = euclidEsmProvider;
        this.euclidEnsProvider = euclidEnsProvider;
        this.euclidEmoProvider = euclidEmoProvider;
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
     * Registers a handler for the object events of {@code bucketName} that match the filters,
     * delivered through a queue of this listener's own.
     */
    public void registerBucket(Object bean, Method method, String queueName, String bucketName, String prefix,
                               boolean directories, List<String> eventTypes, long maxMessages, long waitTime,
                               long visibilityTimeout, boolean autoDelete, int concurrency) {
        method.setAccessible(true);
        bucketRegistrations.add(new BucketRegistration(bean, method, queueName, bucketName, prefix, directories,
                eventTypes, maxMessages, waitTime, visibilityTimeout, autoDelete, concurrency));
    }

    @Override
    public void start() {
        int listeners = concurrencySum(queueRegistrations) + concurrencySum(topicRegistrations)
                + concurrencySum(bucketRegistrations);
        if (listeners == 0 || !running.compareAndSet(false, true)) {
            return;
        }
        resolveClients();
        startUtilisationReporting();
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
     * Gives a bucket listener a queue of its own, subscribes that queue to the bucket's object
     * events, and then receives from it exactly as a queue listener does.
     *
     * <p>The queue belongs to this run alone - its name carries a per-run id - which is what makes
     * both ends of its life simple: nothing else is receiving from it, so it can be deleted
     * outright on shutdown, and a queue left behind by a run that never got to do that is
     * recognisable to the next one. Delivery is the server's business from then on; the filters
     * ride on the subscription, so only the matching events are ever put in.
     */
    private void startBucketListener(BucketRegistration registration) {
        executor.submit(() -> {
            String queueErn;
            try {
                String bucketErn = euclidEsm.getBucketErn(registration.bucketName()).ern();
                sweepOrphanedQueues(registration, bucketErn);
                queueErn = openBucketDelivery(registration, bucketErn);
            } catch (Exception e) {
                logger.error("Could not subscribe to " + registration.describe() + ", listener not started", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            // This thread becomes the first of the registration's pollers, so the executor holds
            // exactly the concurrency() threads start() sized it for.
            for (int i = 1; i < Math.max(1, registration.concurrency()); i++) {
                executor.submit(() -> pollMessages(registration, queueErn));
            }
            pollMessages(registration, queueErn);
        });
    }

    /**
     * Creates this run's delivery queue and subscribes it to the bucket, filtered.
     *
     * @return the ERN of the queue to receive from
     */
    private String openBucketDelivery(BucketRegistration registration, String bucketErn) throws Exception {
        String queueName = registration.queueName() + "-" + runId;

        // The visibility timeout is the queue's, not the message's: it is what gives a handler
        // that dies mid-work its event back rather than losing it.
        String queueErn = euclidSqs.createQueue(queueName, registration.visibilityTimeout(), DEFAULT_MAX_RETRIES,
                DEFAULT_MAX_MESSAGE_LENGTH, "").ern();
        touchHeartbeat(queueErn);

        String subscriptionErn = euclidEsm.subscribe(bucketErn, "SQS", queueErn, registration.eventTypes(),
                registration.prefix(), registration.directories()).ern();
        bucketDeliveries.put(queueName, new BucketDelivery(queueErn, subscriptionErn));
        scheduleHeartbeat(queueErn);

        logger.info("Listening for " + registration.describe() + " on queue '" + queueName + "'");
        return queueErn;
    }

    /**
     * Deletes the queues an earlier run of this same listener left behind.
     *
     * <p>A queue is only deleted when its heartbeat tag has gone stale, because "left behind by a
     * crash" and "belonging to an instance running right now" look identical from the outside
     * otherwise - both are queues of this application, this bucket and this method, with a
     * different run id. The heartbeat is what separates them, and being generous about how stale
     * is stale costs only a queue that outlives its process by a few minutes, where being wrong
     * the other way would delete a live listener's deliveries.
     */
    private void sweepOrphanedQueues(BucketRegistration registration, String bucketErn) {
        try {
            String queuePrefix = registration.queueName() + "-";
            List<Queue> queues = euclidSqs.listQueues(queuePrefix, SWEEP_PAGE_SIZE, 0, "name").queues();
            Instant stale = Instant.now().minusSeconds(HEARTBEAT_STALE_SECONDS);
            Set<String> keptQueueErns = new HashSet<>();
            for (Queue queue : queues == null ? List.<Queue>of() : queues) {
                if (queue.name() == null || queue.name().endsWith("-" + runId) || !isStale(queue, stale)) {
                    if (queue.ern() != null) {
                        keptQueueErns.add(queue.ern());
                    }
                    continue;
                }
                euclidSqs.deleteQueue(queue.ern());
                logger.info("Deleted queue '" + queue.name() + "', left behind by an earlier run of "
                        + registration.describe());
            }
            sweepOrphanedSubscriptions(registration, bucketErn, queuePrefix, keptQueueErns);
        } catch (Exception e) {
            // A sweep that fails costs a queue nobody drains, not a listener: the one being
            // started has its own and works regardless.
            logger.warn("Could not sweep queues left behind by " + registration.describe(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Removes the bucket subscriptions that deliver into queues which no longer exist.
     *
     * <p>Deleting the queue is not enough: the subscription is what the server fans an event out
     * to, so one left pointing at a deleted queue makes every upload into this bucket produce a
     * "target queue not found" and a little wasted work, for ever. Since a subscription outlives
     * the process that made it and its ERN is only known to that process, an abandoned one can be
     * recognised only from the outside - by the queue it names being gone.
     *
     * <p>Scoped to this listener's own queues, by name: a bucket may be subscribed by other
     * applications whose queues are none of this one's business. Deletions made by the sweep above
     * are covered too, since those queues are gone by the time this runs.
     */
    private void sweepOrphanedSubscriptions(BucketRegistration registration, String bucketErn,
                                            String queuePrefix, Set<String> keptQueueErns)
            throws IOException, InterruptedException {
        List<de.jensvogt.euclid.dto.esm.model.Subscription> subscriptions =
                euclidEsm.listSubscriptions(bucketErn).subscriptions();
        if (subscriptions == null) {
            return;
        }
        for (de.jensvogt.euclid.dto.esm.model.Subscription subscription : subscriptions) {
            String targetErn = subscription.targetErn();
            if (targetErn == null || keptQueueErns.contains(targetErn)) {
                continue;
            }
            String queueName = targetErn.substring(targetErn.lastIndexOf(':') + 1);
            if (!queueName.startsWith(queuePrefix) || queueName.endsWith("-" + runId)) {
                continue;
            }
            euclidEsm.unsubscribe(subscription.ern());
            logger.info("Deleted subscription of queue '" + queueName + "', left behind by an earlier run of "
                    + registration.describe());
        }
    }

    /**
     * Whether a queue's owner has stopped saying it is alive - or never said so, in which case the
     * queue's own age decides, so one abandoned between being created and its first heartbeat is
     * swept too.
     */
    private static boolean isStale(Queue queue, Instant stale) {
        String beat = queue.tags() == null ? null : queue.tags().get(HEARTBEAT_TAG);
        String timestamp = StringUtils.hasText(beat) ? beat : queue.created();
        if (!StringUtils.hasText(timestamp)) {
            return false;
        }
        try {
            return Instant.parse(timestamp).isBefore(stale);
        } catch (DateTimeParseException e) {
            // Unreadable is not evidence of death, and deleting on a timestamp this cannot parse
            // would be the one failure here that loses another listener's events.
            return false;
        }
    }

    /**
     * Says this run is still alive, often enough that {@link #HEARTBEAT_STALE_SECONDS} is several
     * missed beats rather than one.
     */
    private void scheduleHeartbeat(String queueErn) {
        ensureScheduler();
        heartbeats.scheduleWithFixedDelay(() -> touchHeartbeat(queueErn),
                HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    private void touchHeartbeat(String queueErn) {
        try {
            euclidSqsInternal.setQueueTag(queueErn, HEARTBEAT_TAG, Instant.now().toString());
        } catch (Exception e) {
            logger.debug("Could not refresh the heartbeat of queue " + queueErn, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

        /**
     * How often utilisation is reported, in seconds.
     *
     * <p>Matched to the autoscaler's own reconcile cadence: reporting faster gives it nothing to
     * act on sooner, and reporting much slower would let a burst come and go between two samples.
     */
    private static final long UTILISATION_PERIOD_SECONDS = 15;

    /**
     * Reports how hard this instance is working, so the autoscaler can act on it.
     *
     * <p>Only when running as a euclid application: {@code EUCLID_INSTANCE_ID} is set by the
     * manager for the process it started, and without it there is no instance for a measurement to
     * be about - a listener running on a developer's laptop has nothing to scale.
     */
    private void startUtilisationReporting() {

        final String instanceId = System.getenv("EUCLID_INSTANCE_ID");
        final String applicationId = System.getenv("EUCLID_APPLICATION_ID");
        if (instanceId == null || instanceId.isBlank()) {
            // Said out loud, not at debug. This fires once, so it cannot become noise - and when
            // load reporting is silently absent, this is the line that explains why. An
            // application deployed through euclid always has this set; a process started by hand
            // never does, and for that one it is genuinely just information.
            logger.info("Not reporting load: EUCLID_INSTANCE_ID is not set, so this process was not started by euclid "
                                + "as an application instance and there is no instance for a measurement to be about");
            return;
        }

        final EuclidEmo euclidEmo = euclidEmoProvider.getIfAvailable();
        if (euclidEmo == null) {
            // A warning rather than information: this process *is* a euclid application instance,
            // so load reporting was meant to happen and something is wrong with the wiring.
            logger.warn("Not reporting load for instance " + instanceId + ": no EuclidEmo bean is available, so the "
                                + "autoscaler will see nothing from this instance");
            return;
        }

        final int capacity = concurrencySum(queueRegistrations) + concurrencySum(topicRegistrations)
                             + concurrencySum(bucketRegistrations);
        ensureScheduler();
        heartbeats.scheduleAtFixedRate(() -> {
            try {
                long now = System.nanoTime();
                long windowStart = windowStartNanos.getAndSet(now);
                long busy = busyNanos.getAndSet(0);
                long elapsed = now - windowStart;
                if (elapsed <= 0 || capacity <= 0) {
                    return;
                }

                // Against capacity, not the clock: a listener sized for eight concurrent handlers
                // with four of them running is half loaded, not fully. Dividing by elapsed alone
                // would report 400% and make every multi-threaded listener look saturated the
                // moment two messages arrived together.
                double utilisation = 100.0 * busy / ((double) elapsed * capacity);

                // Utilisation alone cannot tell "working through a burst, nearly done" from
                // "cannot keep up": both read as busy. The depth of what is still waiting is the
                // other half, and this process is the only party that knows which queues those
                // are - a bucket listener's delivery queue is created at runtime and named after
                // this run.
                //
                // Labelled by instance like the utilisation beside it, not by application: EMO
                // averages the samples that share a label, so three instances reporting their own
                // depth under one application label would report the mean rather than the total.
                // The reader sums the series instead.
                if (loadReportFailed.compareAndSet(true, false)) {
                    logger.info("Load reporting for instance " + instanceId + " is working again");
                }
                euclidEmo.pushMetrics(applicationId == null ? "application" : applicationId,
                                      List.of(Metric.gauge("application-utilisation", "instance", instanceId,
                                                           Math.min(100.0, utilisation)),
                                              Metric.gauge("application-backlog", "instance", instanceId,
                                                           backlog())));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // Never fatal: the listeners are the point of this process, and a monitoring push
                // that cannot get through must not stop them. But the first failure is said out
                // loud - a reporter that never succeeds looks exactly like one that was never
                // started, and that is a difference worth one log line. Every failure after it
                // drops to debug, so a broken endpoint cannot fill the log.
                if (loadReportFailed.compareAndSet(false, true)) {
                    logger.warn("Could not report load for instance " + instanceId
                                        + "; the autoscaler will not see this instance until it succeeds", e);
                } else {
                    logger.debug("Could not report load", e);
                }
            }
        }, UTILISATION_PERIOD_SECONDS, UTILISATION_PERIOD_SECONDS, TimeUnit.SECONDS);

        logger.info("Reporting load for instance "+instanceId+" every "+UTILISATION_PERIOD_SECONDS+"s, capacity "+capacity+" concurrent handlers");
    }

        /**
     * How many messages are waiting across the queues this process polls.
     *
     * <p>Best effort: a queue that cannot be counted contributes nothing rather than failing the
     * whole report, since a backlog that is missing one queue is still far more useful than no
     * backlog at all.
     *
     * @return the total available message count
     */
    private double backlog() {
        long total = 0;
        for (String ern : polledQueueErns) {
            try {
                total += euclidSqsInternal.getMessageCount(ern).available();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return total;
            } catch (Exception e) {
                logger.debug("Could not count messages of " + ern, e);
            }
        }
        return total;
    }

    /**
     * Creates the heartbeat scheduler on first use. One thread for every bucket listener: a beat
     * is a single tag write, and a container with no bucket listeners should not hold a thread it
     * never uses.
     */
    private void ensureScheduler() {
        if (heartbeats == null) {
            heartbeats = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "euclid-listener-heartbeat");
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
        if (heartbeats != null) {
            heartbeats.shutdownNow();
            heartbeats = null;
        }
        closeBucketDeliveries();
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Unsubscribes and deletes what each bucket listener created. Both go, and in that order: a
     * subscription outliving its queue makes the server deliver to somewhere nothing reads, and a
     * queue outliving this process is one nobody will ever drain.
     *
     * <p>Failing here is logged and stepped over rather than thrown. A context is closing, the
     * next start sweeps whatever is left anyway, and one queue that would not delete should not
     * stop the others from being cleaned up.
     */
    private void closeBucketDeliveries() {
        for (Map.Entry<String, BucketDelivery> entry : bucketDeliveries.entrySet()) {
            BucketDelivery delivery = entry.getValue();
            try {
                euclidEsm.unsubscribe(delivery.subscriptionErn());
            } catch (Exception e) {
                logger.warn("Could not unsubscribe queue '" + entry.getKey() + "' from its bucket", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                euclidSqs.deleteQueue(delivery.queueErn());
            } catch (Exception e) {
                logger.warn("Could not delete queue '" + entry.getKey() + "'", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        bucketDeliveries.clear();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Resolves the clients the registered listeners need, and only those: an application whose
     * listeners are all {@code @QueueListener} never asks for an ENS or ESM client, so it never
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
            euclidSqs = euclidSqsProvider.getObject();
            euclidEsm = euclidEsmProvider.getObject();
        }
        if (euclidSqs != null) {
            euclidSqsInternal = euclidSqs.asInternal();
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

    private void dispatch(MessageRegistration registration, Message message) {
        try {
            Method method = registration.method();
            Class<?> paramType = method.getParameterTypes()[0];
            Object arg = registration instanceof BucketRegistration
                    ? bucketArgument(message, paramType)
                    : messageArgument(message, paramType);
            // Timed around the handler alone, not around the receive that delivered the message:
            // a long poll waiting for work is the opposite of being busy, and counting it would
            // make an idle listener look fully loaded.
            long startedAt = System.nanoTime();
            try {
                method.invoke(registration.bean(), arg);
            } finally {
                busyNanos.addAndGet(System.nanoTime() - startedAt);
            }
            if (registration.autoDelete()) {
                euclidSqs.deleteMessage(message.receiptHandle());
            }
        } catch (Exception e) {
            logger.error("Listener for " + registration.describe() + " failed to handle message "
                    + message.ern(), e);
        }
    }

    /**
     * The argument a queue or topic handler is called with: the body, the envelope, or the body
     * read as the type the handler names.
     */
    private Object messageArgument(Message message, Class<?> paramType) {
        if (paramType == String.class) {
            return message.body();
        }
        if (paramType.isAssignableFrom(Message.class)) {
            return message;
        }
        return jsonMapper.readValue(message.body(), paramType);
    }

    /**
     * The argument a bucket handler is called with, rebuilt from the message the subscription
     * delivered so that handlers keep the {@code Event} they were written against.
     *
     * <p>The pieces come from both halves of the message: the notification's fields are its body,
     * while the event id and the delivery count are the queue's own - {@code messageId} names this
     * delivery, and {@code receivedCount} is what tells a first delivery from a redelivery of one
     * that failed or timed out. The event type is read from a message attribute when the server
     * sets one, falling back to the field the notification body carries.
     */
    private Object bucketArgument(Message message, Class<?> paramType) {
        Map<String, Object> payload = readPayload(message);
        if (paramType.isAssignableFrom(Map.class)) {
            return payload;
        }
        if (paramType.isAssignableFrom(Event.class)) {
            return new Event(message.messageId(), eventType(message, payload), "esm", payload,
                    message.receivedCount(), message.created());
        }
        return payloadMapper.convertValue(payload, paramType);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readPayload(Message message) {
        return payloadMapper.readValue(message.body(), Map.class);
    }

    private static String eventType(Message message, Map<String, Object> payload) {
        Map<String, Variant> attributes = message.attributes();
        Variant attribute = attributes == null ? null : attributes.get(EVENT_TYPE_ATTRIBUTE);
        if (attribute != null && attribute.value() != null) {
            return String.valueOf(attribute.value());
        }
        Object fromBody = payload.get(EVENT_TYPE_ATTRIBUTE);
        return fromBody == null ? null : String.valueOf(fromBody);
    }

    /**
     * A registration that says how many threads it wants, which is all sizing the executor needs
     * to know about any of them.
     */
    private interface HasConcurrency {

        int concurrency();
    }

    /**
     * What the two message-driven listeners have in common: both receive from a queue and differ
     * only in how that queue is arrived at - named outright, or subscribed to a topic.
     */
    private sealed interface MessageRegistration extends HasConcurrency permits QueueRegistration, TopicRegistration, BucketRegistration {

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

    private record BucketRegistration(Object bean, Method method, String queueName, String bucketName, String prefix,
                                      boolean directories, List<String> eventTypes, long maxMessages, long waitTime,
                                      long visibilityTimeout, boolean autoDelete, int concurrency)
            implements MessageRegistration {

        @Override
        public String describe() {
            return "bucket '" + bucketName + "'";
        }
    }

    /**
     * What a bucket listener created and therefore has to take down again.
     */
    private record BucketDelivery(String queueErn, String subscriptionErn) {
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
        polledQueueErns.add(ern);
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
}
