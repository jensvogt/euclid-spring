package de.jensvogt.euclid.spring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.StringUtils;

import jakarta.annotation.PostConstruct;

/**
 * Says so when euclid was not configured, instead of letting the application find out later.
 *
 * <p>{@link EuclidSqsAutoConfiguration} is conditional on {@code euclid.base-url}, so an
 * application started without it - a jar run without the profile that sets it, a container missing
 * an environment variable - gets no euclid beans at all. What it gets instead is silence, and then
 * either a listener that never fires or a startup failure naming whichever client its own code
 * asked for first:
 *
 * <pre>
 * Parameter 0 of constructor in com.example.MyAdapter required a bean of type
 * 'de.jensvogt.euclid.module.esm.EuclidEsm' that could not be found.
 * </pre>
 *
 * <p>That message is true and useless: the missing bean is a symptom, and the cause is one absent
 * property several layers away. This class exists only to name the cause, at the moment it can
 * still be acted on.
 *
 * <p>Its condition is the mirror image of the one it warns about, so exactly one of the two
 * configurations is ever active. It is written out by hand rather than as
 * {@code @ConditionalOnProperty(..., havingValue = "", matchIfMissing = true)}, because Spring
 * treats an empty {@code havingValue} as none given - that spelling matches whether or not the
 * property is set, and would warn every application including the configured ones.
 */
@AutoConfiguration
@Conditional(EuclidNotConfiguredWarning.OnMissingBaseUrl.class)
public class EuclidNotConfiguredWarning {

    /**
     * Matches when neither {@code euclid.base-url} nor {@code euclid.endpoint} says where the
     * gateway is - blank counts as absent, since an endpoint of "" configures nothing either.
     */
    static class OnMissingBaseUrl implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !StringUtils.hasText(context.getEnvironment().getProperty("euclid.base-url"))
                    && !StringUtils.hasText(context.getEnvironment().getProperty("euclid.endpoint"));
        }
    }

    private static final Log logger = LogFactory.getLog(EuclidNotConfiguredWarning.class);

    /**
     * Creates the warning. Nothing else is registered: this configuration is a message, not
     * infrastructure.
     */
    public EuclidNotConfiguredWarning() {
    }

    /**
     * Logs what is missing and what it costs.
     */
    @PostConstruct
    public void warn() {
        logger.warn("Neither euclid.base-url (EUCLID_BASE_URL) nor euclid.endpoint (EUCLID_ENDPOINT) is set, "
                + "so no euclid clients were configured: "
                + "EuclidEqs, EuclidEsm, EuclidEes, EuclidEns and every @QueueListener, @TopicListener "
                + "and @BucketListener are absent. Set euclid.base-url (EUCLID_BASE_URL), or activate the "
                + "profile that sets it - for a packaged jar that means passing it explicitly, e.g. "
                + "-Dspring.profiles.active=local, since a profile the IDE applies is not applied by java -jar.");
    }
}
