package de.jensvogt.euclid.spring.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a handler for the object events of an Euclid ESM bucket, named the way the
 * bucket itself is named. The annotated bean must be a Spring bean;
 * {@code euclid-spring-boot-starter}'s {@code BucketListenerBeanPostProcessor} discovers annotated
 * methods and registers them with the {@code EuclidListenerContainer}.
 *
 * <p>At startup the container gives the listener a queue of its own, subscribes that queue to the
 * bucket's object events - filtered by {@link #eventTypes()}, {@link #prefix()} and
 * {@link #directories()} - and receives from it exactly as {@code @QueueListener} does, websocket
 * wake-up included. The filters ride on the subscription and the server applies them as it
 * publishes, so only matching events are ever put in the queue.
 *
 * <p>By default the queue belongs to one run of the application: it is created at startup, and
 * deleted along with its subscription when the context closes. A queue left behind by a run that
 * was killed before it could do that is deleted by the next run's sweep, once its owner has stopped
 * saying it is alive. Two consequences follow. Events published while no instance is running are
 * not kept - there is no queue to keep them in - and every running instance has its own queue, so
 * each receives every event rather than the instances sharing the work between them.
 *
 * <p>Set {@link #shared()} to divide the work instead: one queue for the listener, competed for by
 * every instance. See there - it is the right setting for anything scaled for throughput.
 *
 * <p>Supported method signatures: {@code (Event event)} (the full envelope, including event type,
 * event id, delivery attempts and the raw payload map), {@code (Map<String, Object> payload)}, or
 * {@code (T payload)} for any other type {@code T}, in which case the payload is converted into
 * {@code T} via Jackson.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BucketListener {

    /**
     * Name of the bucket to listen on, matched against the events' {@code bucketName}.
     */
    String value();

    /**
     * Key prefix to restrict the events to, e.g. {@code "reports/"} for one "directory", or empty
     * for the whole bucket. Matched against the events' {@code prefix}, which the server derives
     * from the key, so it names a directory exactly rather than any key starting with these
     * characters.
     */
    String prefix() default "";

    /**
     * Whether events for directory markers - the zero-byte objects an FTP {@code MKD} leaves
     * behind - are delivered too.
     */
    boolean directories() default false;

    /**
     * Event types to receive, by default every object event ESM publishes.
     */
    String[] eventTypes() default {"esm.object.created", "esm.object.updated", "esm.object.deleted"};

    /**
     * Name the delivery queue is built from, defaulting to
     * {@code <spring.application.name>-<bucket>-<method>}. A per-run id is appended to whatever
     * this is, so naming it does not make two runs share a queue - it only decides what the queue
     * is called and, with it, which queues a sweep recognises as this listener's.
     */
    String queue() default "";

    /**
     * Whether every instance of this application shares one delivery queue.
     *
     * <p>False, the default, gives each run its own queue subscribed to the bucket - so every
     * instance receives every event. That is broadcast, and it is what a listener wants when each
     * instance has to act on each event: invalidating a cache it holds, reloading a file it keeps
     * open.
     *
     * <p>True gives the listener one queue with a stable name, shared by every instance, and
     * subscribed once. Instances then compete for events and the work is divided between them,
     * which is what a listener wants when handling an event is the work - parsing a file, storing a
     * record. Two further consequences follow, both improvements: events published while no
     * instance is running are kept, because the queue outlives the run; and the queue is not
     * deleted on shutdown, because other instances may still be reading it.
     *
     * <p>Set this on any listener you scale for throughput. Left false, adding instances multiplies
     * the work rather than dividing it: sixteen instances each parse the same file. Worse, the
     * per-run arrangement has a race under a pool that starts several instances at once - each new
     * run sweeps the subscriptions of queues it did not see when it listed them a moment earlier,
     * so siblings that subscribed in between are unsubscribed, and the last one to start ends up
     * the only one receiving anything. Observed on a parsing pool: seventeen delivery queues, one
     * subscription, 423,444 events waiting in it and fifteen instances polling queues nothing fed.
     */
    boolean shared() default false;

    /**
     * Max number of events fetched per poll.
     */
    long maxMessages() default 10;

    /**
     * Long-poll wait time in seconds passed to {@code EuclidEqs#receiveMessages}, which spends it
     * waiting for the delivery queue's {@code eqs.message.sent} websocket event.
     *
     * <p>Keep this above zero: {@code receiveMessages} does not wait at all for a
     * {@code waitTime} of zero, which turns the listener's loop into a busy one.
     */
    long waitTime() default 20;

    /**
     * Seconds the delivery queue makes a received event invisible for, so a handler that dies
     * mid-work has its event delivered again rather than lost.
     */
    long visibilityTimeout() default 300;

    /**
     * Whether the event is deleted from the delivery queue after the handler returns without
     * throwing. A handler that throws leaves its event in the queue, to be delivered again once
     * the visibility timeout expires.
     */
    boolean autoDelete() default true;

    /**
     * Number of threads receiving from the delivery queue concurrently, each independently
     * claiming and dispatching events. Raise this when publishing outruns what one thread can
     * drain, or a handler is slow enough that throughput matters more than the order events are
     * handled in.
     *
     * <p>Left at zero, the application-wide {@code euclid.listener.concurrency} decides, which is
     * one unless it says otherwise.
     */
    int concurrency() default 0;
}
