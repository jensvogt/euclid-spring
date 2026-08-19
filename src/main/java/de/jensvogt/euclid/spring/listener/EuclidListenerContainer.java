package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.dto.sqs.model.Message;
import de.jensvogt.euclid.module.sqs.EuclidSqs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls Euclid SQS queues on behalf of {@code @QueueListener}-annotated methods registered by
 * {@code QueueListenerBeanPostProcessor}, dispatching received messages to their handler and
 * deleting them from the queue afterward (unless {@code autoDelete = false}).
 *
 * <p>Starts one polling thread per registration when the Spring context is refreshed, and stops
 * them all on context close.
 */
public class EuclidListenerContainer implements SmartLifecycle {

    private static final Log logger = LogFactory.getLog(EuclidListenerContainer.class);

    private final EuclidSqs euclidSqs;
    private final List<Registration> registrations = new ArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public EuclidListenerContainer(EuclidSqs euclidSqs) {
        this.euclidSqs = euclidSqs;
    }

    public void register(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                          boolean autoDelete) {
        method.setAccessible(true);
        registrations.add(new Registration(bean, method, queueName, maxMessages, waitTime, autoDelete));
    }

    @Override
    public void start() {
        if (registrations.isEmpty() || !running.compareAndSet(false, true)) {
            return;
        }
        executor = Executors.newFixedThreadPool(registrations.size(), runnable -> {
            Thread thread = new Thread(runnable, "euclid-listener");
            thread.setDaemon(true);
            return thread;
        });
        for (Registration registration : registrations) {
            executor.submit(() -> pollLoop(registration));
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
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

    private void pollLoop(Registration registration) {
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

    private void dispatch(Registration registration, Message message) {
        try {
            Method method = registration.method();
            Object[] args = method.getParameterCount() == 1 && method.getParameterTypes()[0] == String.class
                    ? new Object[]{message.body()}
                    : new Object[]{message};
            method.invoke(registration.bean(), args);
            if (registration.autoDelete()) {
                euclidSqs.deleteMessage(message.receiptHandle());
            }
        } catch (Exception e) {
            logger.error("Listener for queue '" + registration.queueName() + "' failed to handle message "
                    + message.ern(), e);
        }
    }

    private record Registration(Object bean, Method method, String queueName, long maxMessages, long waitTime,
                                 boolean autoDelete) {
    }
}
