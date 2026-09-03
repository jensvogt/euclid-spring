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
 * <p>Events arrive over the Euclid event bus (EES): at startup the container registers a durable
 * subscription for {@link #subscriber()} on {@link #eventTypes()}, filtered to the bucket - and to
 * {@link #prefix()} and {@link #directories()} if those narrow it further - claims the events it
 * matched, and acknowledges them once the handler returns. Because the filter is applied when an
 * event is published, the subscriber only ever accumulates the events it asked for, and because
 * events are stored per subscriber, nothing is lost while the application is down and no queue has
 * to exist.
 *
 * <p>The claim is triggered by the gateway saying that something is waiting, over the websocket
 * connection the auto-configuration opens, so a handler runs as the object changes rather than on
 * the next poll. Where that connection cannot be made - websockets disabled, a proxy in the way, an
 * older server - the container asks on a long poll instead, which is the same delivery a little
 * later. Two things keep polling either way: {@code autoAck = false}, because a listener that
 * acknowledges events itself cannot be driven by one that acknowledges them for it, and an
 * application with no event stream bean.
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
     * Durable subscriber name the events are stored and claimed under, defaulting to
     * {@code <spring.application.name>-<bucket>-<method>}.
     *
     * <p>The name decides fan-out: instances sharing it share the work, since whichever claims an
     * event first handles it, while two names each receive their own copy of every event. Set it
     * explicitly to have separate handlers - or separate applications - deliberately share one
     * subscription.
     */
    String subscriber() default "";

    /**
     * Max number of events claimed per poll.
     */
    long maxEvents() default 10;

    /**
     * Long-poll wait time in seconds passed to {@code EuclidEes#receiveEvents}. The server clamps
     * this to 20.
     */
    long waitTime() default 20;

    /**
     * Seconds a claim holds before an unacknowledged event becomes claimable again, so a handler
     * that dies mid-work has its event redelivered rather than lost.
     */
    long visibilityTimeout() default 300;

    /**
     * Whether the event is acknowledged - and so deleted - after the handler returns without
     * throwing. A handler that throws leaves its event unacknowledged, to be redelivered once the
     * claim expires.
     */
    boolean autoAck() default true;

    /**
     * Number of threads claiming events concurrently, whether that means independently polling -
     * when the event stream is unavailable, or {@code autoAck = false} - or independently draining
     * claims pushed over the event stream. Raise this when publishing outruns what one thread can
     * drain. Either way the connection itself stays a single one; only the claiming and dispatching
     * behind it is parallelized.
     */
    int concurrency() default 1;
}
