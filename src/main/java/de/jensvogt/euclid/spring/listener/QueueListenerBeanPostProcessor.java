package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.spring.annotation.QueueListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

/**
 * Scans every Spring bean for {@code @QueueListener}-annotated methods and registers them with
 * the {@link EuclidListenerContainer}, the way {@code @KafkaListener}/{@code @JmsListener} are
 * discovered in their respective Spring integrations.
 */
public class QueueListenerBeanPostProcessor implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final ObjectProvider<EuclidListenerContainer> container;
    private StringValueResolver embeddedValueResolver;

    /**
     * Takes the container as a provider rather than the container itself: a bean post processor is
     * created before the ordinary singletons are, and asking for the container there would drag
     * the whole client graph into that phase. It is only needed once an annotated method is
     * actually found, which is after the context has finished starting up.
     */
    public QueueListenerBeanPostProcessor(ObjectProvider<EuclidListenerContainer> container) {
        this.container = container;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            QueueListener annotation = method.getAnnotation(QueueListener.class);
            String queueName = embeddedValueResolver != null
                    ? embeddedValueResolver.resolveStringValue(annotation.value())
                    : annotation.value();
            container.getObject().register(bean, method, queueName, annotation.maxMessages(), annotation.waitTime(),
                    annotation.autoDelete(), annotation.concurrency());
        }, method -> method.getAnnotation(QueueListener.class) != null);
        return bean;
    }
}
