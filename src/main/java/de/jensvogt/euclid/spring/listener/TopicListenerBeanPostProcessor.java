package de.jensvogt.euclid.spring.listener;

import de.jensvogt.euclid.spring.annotation.TopicListener;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;

/**
 * Scans every Spring bean for {@code @TopicListener}-annotated methods and registers them with the
 * {@link EuclidListenerContainer}, the counterpart of {@link QueueListenerBeanPostProcessor} for
 * ENS topics.
 */
public class TopicListenerBeanPostProcessor implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final ObjectProvider<EuclidListenerContainer> container;
    private StringValueResolver embeddedValueResolver;

    /**
     * Takes the container as a provider rather than the container itself: a bean post processor is
     * created before the ordinary singletons are, and asking for the container there would drag
     * the whole client graph into that phase. It is only needed once an annotated method is
     * actually found, which is after the context has finished starting up.
     */
    public TopicListenerBeanPostProcessor(ObjectProvider<EuclidListenerContainer> container) {
        this.container = container;
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            TopicListener annotation = method.getAnnotation(TopicListener.class);
            String topic = resolve(annotation.value());
            container.getObject().registerTopic(bean, method, topic, queue(annotation, topic, method),
                    annotation.maxMessages(), annotation.waitTime(), annotation.autoDelete());
        }, method -> method.getAnnotation(TopicListener.class) != null);
        return bean;
    }

    /**
     * The queue the topic is delivered to, which decides fan-out: derived from the method unless
     * named explicitly, so that two instances of one application share one queue - they run the
     * same method - while two handlers of the same topic each get their own copy of every message.
     */
    private String queue(TopicListener annotation, String topic, Method method) {
        if (StringUtils.hasText(annotation.queue())) {
            return resolve(annotation.queue());
        }
        String applicationName = resolve("${spring.application.name:}");
        String name = topic + "-" + method.getName();
        return StringUtils.hasText(applicationName) ? applicationName + "-" + name : name;
    }

    private String resolve(String value) {
        return embeddedValueResolver != null ? embeddedValueResolver.resolveStringValue(value) : value;
    }
}
