package de.jensvogt.euclid.spring.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a handler for the messages published to an Euclid ENS topic, named the way the
 * topic itself is named. The annotated bean must be a Spring bean;
 * {@code euclid-spring-boot-starter}'s {@code TopicListenerBeanPostProcessor} discovers annotated
 * methods and registers them with the {@code EuclidListenerContainer}.
 *
 * <p>A topic is not read directly - it fans out to the queues subscribed to it, which is the only
 * delivery ENS has. So at startup the container resolves the topic's ERN via
 * {@code EuclidEns#getTopicErn}, makes sure the delivery {@link #queue()} exists, subscribes it to
 * the topic unless a subscription to that queue is already registered, and from then on receives
 * from that queue exactly as {@code @QueueListener} does - including waking on the queue's
 * {@code eqs.message.sent} websocket event rather than on a poll tick.
 *
 * <p>The queue is what decides fan-out, so it defaults to one per handler:
 * {@code <spring.application.name>-<topic>-<method>}. Two instances of one application therefore
 * share a queue and split the messages between them, while two handlers - or two applications -
 * each subscribe their own queue and so each receive every message, which is what publishing to a
 * topic is for.
 *
 * <p>Supported method signatures: {@code (Message message)} (the full envelope, including the
 * attributes published with the message and the receipt handle), {@code (String body)} (the raw
 * message body), or {@code (T payload)} for any other type {@code T}, in which case the message
 * body is deserialized as JSON into {@code T} via Jackson.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TopicListener {

    /**
     * Name of the topic to listen on, resolved to its ERN via {@code EuclidEns#getTopicErn} at
     * startup.
     */
    String value();

    /**
     * Name of the queue the topic's messages are delivered to, created if it does not exist yet.
     * Defaults to {@code <spring.application.name>-<topic>-<method>}.
     */
    String queue() default "";

    /**
     * Max number of messages fetched per poll.
     */
    long maxMessages() default 10;

    /**
     * Long-poll wait time in seconds passed to {@code EuclidEqs#receiveMessages}, which spends it
     * waiting for an {@code eqs.message.sent} websocket event for the delivery queue.
     *
     * <p>Keep this above zero: {@code receiveMessages} does not wait at all for a
     * {@code waitTime} of zero, which turns the listener's loop into a busy one.
     */
    long waitTime() default 20;

    /**
     * Whether the message is deleted from the delivery queue after the handler returns without
     * throwing.
     */
    boolean autoDelete() default true;

    /**
     * Number of threads polling the delivery queue concurrently, each independently claiming and
     * dispatching messages. Raise this when publishing outruns what one thread can drain, or a
     * handler is slow enough that throughput matters more than order - a message claimed by a
     * concurrent thread is no longer guaranteed to be handled in the order it was published, since
     * a slower thread can still be working on an older message while a faster one moves on to a
     * newer one.
     */
    int concurrency() default 1;
}
