package de.jensvogt.euclid.spring;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The warning has to fire exactly when the clients are missing, and never when they are there -
 * otherwise it is either noise in every healthy application or silence in the one case it exists
 * for. Its condition is the mirror image of {@link EuclidSqsAutoConfiguration}'s, so this pins the
 * two against each other rather than testing the log line.
 */
class EuclidNotConfiguredWarningTest {

    @Test
    void withoutABaseUrlTheWarningIsActive() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(EuclidNotConfiguredWarning.class);
            context.refresh();

            // Nothing configured euclid, so something has to say so - this is the case where an
            // application otherwise starts and quietly does nothing, or dies naming a bean that
            // was never the point.
            assertTrue(context.containsBean(beanName()), "the warning did not fire without euclid.base-url");
        }
    }

    @Test
    void withOnlyTheManagersEndpointTheWarningStaysQuiet() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(
                    new MapPropertySource("test", Map.of("euclid.endpoint", "https://localhost:5566")));
            context.register(EuclidNotConfiguredWarning.class);
            context.refresh();

            // An application euclid deployed is given EUCLID_ENDPOINT and never sets base-url, and
            // it is configured perfectly well - warning it would be wrong.
            assertFalse(context.containsBean(beanName()), "the warning fired for an application euclid configured");
        }
    }

    @Test
    void withABaseUrlTheWarningStaysQuiet() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(
                    new MapPropertySource("test", Map.of("euclid.base-url", "https://localhost:5566")));
            context.register(EuclidNotConfiguredWarning.class);
            context.refresh();

            assertFalse(context.containsBean(beanName()), "the warning fired even though euclid is configured");
        }
    }

    private static String beanName() {
        return "euclidNotConfiguredWarning";
    }
}
