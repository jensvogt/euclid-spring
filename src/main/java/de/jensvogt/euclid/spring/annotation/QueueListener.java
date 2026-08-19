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
 * <p>Supported method signatures: {@code (Message message)} or {@code (String body)}.
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
     * Long-poll wait time in seconds passed to {@code EuclidSqs#receiveMessages}.
     */
    long waitTime() default 20;

    /**
     * Whether the message is deleted from the queue after the handler returns without throwing.
     */
    boolean autoDelete() default true;
}
