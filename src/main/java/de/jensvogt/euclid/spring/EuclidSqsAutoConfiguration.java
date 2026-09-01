package de.jensvogt.euclid.spring;

import de.jensvogt.euclid.Euclid;
import de.jensvogt.euclid.module.eam.EuclidEam;
import de.jensvogt.euclid.module.eam.EuclidSession;
import de.jensvogt.euclid.module.ees.EuclidEes;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import de.jensvogt.euclid.module.esm.EuclidEsm;
import de.jensvogt.euclid.spring.listener.BucketListenerBeanPostProcessor;
import de.jensvogt.euclid.spring.listener.EuclidListenerContainer;
import de.jensvogt.euclid.spring.listener.QueueListenerBeanPostProcessor;
import de.jensvogt.euclid.spring.listener.TopicListenerBeanPostProcessor;
import de.jensvogt.euclid.ws.EuclidEventStream;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;

/**
 * Autoconfigures {@link EuclidEqs}, {@link EuclidEsm}, {@link EuclidEes} and {@link EuclidEns}
 * clients from {@code euclid.*} properties, plus the infrastructure backing {@code @QueueListener},
 * {@code @TopicListener} and {@code @BucketListener}.
 */
@AutoConfiguration
@EnableConfigurationProperties(EuclidProperties.class)
@ConditionalOnProperty(prefix = "euclid", name = "base-url")
public class EuclidSqsAutoConfiguration {

    /**
     * Logs in once, so every module client below is derived from the same session.
     */
    @Bean
    @ConditionalOnMissingBean
    public EuclidSession euclidSession(EuclidProperties properties) throws IOException, InterruptedException {
        EuclidEam euclidEam = Euclid.forServer(properties.getBaseUrl())
                .access()
                .credentials(properties.getUsername(), properties.getPassword());
        if (properties.getCaCertPath() != null) {
            euclidEam.caCertPath(properties.getCaCertPath());
        }
        return euclidEam.login();
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEqs euclidSqs(EuclidSession euclidSession) {
        return euclidSession.eqs();
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEsm euclidEsm(EuclidSession euclidSession) {
        return euclidSession.esm();
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEes euclidEes(EuclidSession euclidSession) {
        return euclidSession.ees();
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEns euclidEns(EuclidSession euclidSession) {
        return euclidSession.ens();
    }

    /**
     * The connection a {@code @BucketListener} is told over. Opened lazily by the first listener
     * that starts, and never opened at all in an application that has none - and if it cannot be
     * opened, the listeners fall back to asking, so this is an optimization rather than a
     * requirement.
     */
    @Bean
    @ConditionalOnMissingBean
    public EuclidEventStream euclidEventStream(EuclidSession euclidSession, EuclidProperties properties) {
        return new EuclidEventStream(properties.getBaseUrl(), euclidSession.token(), euclidSession.region(),
                euclidSession.accountId(), euclidSession.userId(), euclidSession.accessKeyId(),
                euclidSession.secretAccessKey(), properties.getCaCertPath(), "ees");
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidListenerContainer euclidListenerContainer(EuclidEqs euclidSqs, EuclidEes euclidEes,
                                                             EuclidEns euclidEns,
                                                             ObjectProvider<EuclidEventStream> eventStreamProvider,
                                                             ObjectProvider<JsonMapper> objectMapperProvider) {
        return new EuclidListenerContainer(euclidSqs, euclidEes, euclidEns, eventStreamProvider, objectMapperProvider);
    }

    @Bean
    public QueueListenerBeanPostProcessor queueListenerBeanPostProcessor(EuclidListenerContainer container) {
        return new QueueListenerBeanPostProcessor(container);
    }

    @Bean
    public TopicListenerBeanPostProcessor topicListenerBeanPostProcessor(EuclidListenerContainer container) {
        return new TopicListenerBeanPostProcessor(container);
    }

    @Bean
    public BucketListenerBeanPostProcessor bucketListenerBeanPostProcessor(EuclidListenerContainer container) {
        return new BucketListenerBeanPostProcessor(container);
    }
}
