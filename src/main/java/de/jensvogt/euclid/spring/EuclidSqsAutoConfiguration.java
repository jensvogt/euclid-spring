package de.jensvogt.euclid.spring;

import de.jensvogt.euclid.Euclid;
import de.jensvogt.euclid.module.access.EuclidAccess;
import de.jensvogt.euclid.module.sqs.EuclidSqs;
import de.jensvogt.euclid.spring.listener.EuclidListenerContainer;
import de.jensvogt.euclid.spring.listener.QueueListenerBeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

/**
 * Auto-configures an {@link EuclidSqs} client from {@code euclid.*} properties, plus the
 * infrastructure backing {@code @QueueListener}.
 */
@AutoConfiguration
@EnableConfigurationProperties(EuclidProperties.class)
@ConditionalOnProperty(prefix = "euclid", name = "base-url")
public class EuclidSqsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EuclidSqs euclidSqs(EuclidProperties properties) throws IOException, InterruptedException {
        EuclidAccess access = Euclid.forServer(properties.getBaseUrl())
                .access()
                .credentials(properties.getUsername(), properties.getPassword());
        if (properties.getCaCertPath() != null) {
            access.caCertPath(properties.getCaCertPath());
        }
        return access.login().sqs();
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidListenerContainer euclidListenerContainer(EuclidSqs euclidSqs) {
        return new EuclidListenerContainer(euclidSqs);
    }

    @Bean
    public QueueListenerBeanPostProcessor queueListenerBeanPostProcessor(EuclidListenerContainer container) {
        return new QueueListenerBeanPostProcessor(container);
    }
}
