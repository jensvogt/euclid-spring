package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.spring.annotation.BucketListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Scans every Spring bean for {@code @BucketListener}-annotated methods and registers them with
 * the {@link EuclidListenerContainer}, the counterpart of {@link QueueListenerBeanPostProcessor}
 * for ESM bucket events.
 */
public class BucketListenerBeanPostProcessor implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final ObjectProvider<EuclidListenerContainer> container;
    private StringValueResolver embeddedValueResolver;

    /**
     * Takes the container as a provider rather than the container itself: a bean post processor is
     * created before the ordinary singletons are, and asking for the container there would drag
     * the whole client graph into that phase. It is only needed once an annotated method is
     * actually found, which is after the context has finished starting up.
     */
    public BucketListenerBeanPostProcessor(ObjectProvider<EuclidListenerContainer> container) {
        this.container = container;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            BucketListener annotation = method.getAnnotation(BucketListener.class);
            String bucket = resolve(annotation.value());
            List<String> eventTypes = Arrays.stream(annotation.eventTypes()).map(this::resolve).toList();
            container.getObject().registerBucket(bean, method, queue(annotation, bucket, method), bucket,
                    resolve(annotation.prefix()), annotation.directories(), eventTypes, annotation.maxMessages(),
                    annotation.waitTime(), annotation.visibilityTimeout(), annotation.autoDelete(),
                    annotation.concurrency());
        }, method -> method.getAnnotation(BucketListener.class) != null);
        return bean;
    }

    /**
     * The name this listener's delivery queue is built from: derived from the method unless named
     * explicitly, so that the queues of two handlers of the same bucket are told apart - by name
     * and by the sweep that cleans up after a crash.
     */
    private String queue(BucketListener annotation, String bucket, Method method) {
        if (StringUtils.hasText(annotation.queue())) {
            return resolve(annotation.queue());
        }
        String applicationName = resolve("${spring.application.name:}");
        String name = bucket + "-" + method.getName();
        return StringUtils.hasText(applicationName) ? applicationName + "-" + name : name;
    }

    private String resolve(String value) {
        return embeddedValueResolver != null ? embeddedValueResolver.resolveStringValue(value) : value;
    }
}
