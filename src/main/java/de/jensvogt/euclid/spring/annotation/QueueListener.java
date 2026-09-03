package de.jensvogt.euclid.spring.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a handler for messages arriving on an Euclid SQS queue. The annotated bean
 * must be a Spring bean; {@code euclid-spring-boot-starter}'s {@code QueueListenerBeanPostProcessor}
 * discovers annotated methods and registers them with the {@code EuclidListenerContainer}.
 *
 * <p>Supported method signatures: {@code (Message message)} (the full envelope, including
 * attributes and receipt handle), {@code (String body)} (the raw message body), or
 * {@code (T payload)} for any other type {@code T}, in which case the message body is
 * deserialized as JSON into {@code T} via Jackson.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QueueListener {

    /**
     * Name of the queue to listen on, resolved to its ERN via
     * {@code EuclidSqs#getQueueErn(String)} at startup.
     */
    String value();

    /**
     * Max number of messages fetched per poll.
     */
    long maxMessages() default 10;

    /**
     * Long-poll wait time in seconds passed to {@code EuclidEqs#receiveMessages}, which spends it
     * waiting for an {@code eqs.message.sent} websocket event for this queue - so a message is
     * usually picked up as it arrives rather than on the next poll tick, and a queue that stays
     * empty costs one round trip per wait rather than a steady stream of them.
     *
     * <p>Keep this above zero: {@code receiveMessages} does not wait at all for a
     * {@code waitTime} of zero, which turns the listener's loop into a busy one.
     */
    long waitTime() default 20;

    /**
     * Whether the message is deleted from the queue after the handler returns without throwing.
     */
    boolean autoDelete() default true;

    /**
     * Number of threads polling this queue concurrently, each independently claiming and
     * dispatching messages. Raise this when a producer outruns what one thread can drain, or a
     * handler is slow enough that throughput matters more than order - a message claimed by a
     * concurrent thread is no longer guaranteed to be handled in the order it was sent, since a
     * slower thread can still be working on an older message while a faster one moves on to a
     * newer one.
     */
    int concurrency() default 1;
}
