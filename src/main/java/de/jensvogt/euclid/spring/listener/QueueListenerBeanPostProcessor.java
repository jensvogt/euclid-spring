package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.spring.annotation.QueueListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.ReflectionUtils;

/**
 * Scans every Spring bean for {@code @QueueListener}-annotated methods and registers them with
 * the {@link EuclidListenerContainer}, the way {@code @KafkaListener}/{@code @JmsListener} are
 * discovered in their respective Spring integrations.
 */
public class QueueListenerBeanPostProcessor implements BeanPostProcessor {

    private final EuclidListenerContainer container;

    public QueueListenerBeanPostProcessor(EuclidListenerContainer container) {
        this.container = container;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            QueueListener annotation = method.getAnnotation(QueueListener.class);
            container.register(bean, method, annotation.value(), annotation.maxMessages(), annotation.waitTime(),
                    annotation.autoDelete());
        }, method -> method.getAnnotation(QueueListener.class) != null);
        return bean;
    }
}
