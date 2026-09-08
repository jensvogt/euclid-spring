package de.jensvogt.euclid.spring.listener;

import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

/**
 * The settings a listener annotation can leave unsaid, resolved from the application's
 * configuration instead - shared by the three bean post processors so an application tunes all of
 * its listeners in one place rather than annotation by annotation.
 */
final class ListenerDefaults {

    /**
     * How many threads a listener that does not say gets, and the value used when
     * {@code euclid.listener.concurrency} is not set either.
     *
     * <p>One, deliberately. A listener's threads are not a pool it borrows from: each is dedicated
     * to that listener for the life of the application and holds a long poll open the whole time,
     * so a higher default would multiply the requests parked against EQS by every listener in
     * every application, busy or idle. One thread also keeps a queue's messages being handled in
     * the order they arrived, which more than one cannot promise.
     */
    static final int DEFAULT_CONCURRENCY = 1;

    private static final String CONCURRENCY_PROPERTY = "${euclid.listener.concurrency:"
            + DEFAULT_CONCURRENCY + "}";

    private ListenerDefaults() {
    }

    /**
     * The number of threads a listener should run, taking the annotation's own value when it names
     * one and the application-wide default when it does not.
     *
     * @param annotated the annotation's {@code concurrency}, zero or less meaning "not said"
     * @param resolver  resolves the property placeholder, or {@code null} outside a context
     * @return a thread count of at least one
     */
    static int concurrency(int annotated, StringValueResolver resolver) {
        if (annotated > 0) {
            return annotated;
        }
        return Math.max(1, configured(resolver));
    }

    /**
     * {@code euclid.listener.concurrency}, or the default when it is unset or not a number - a
     * misspelled value should leave an application running the way it ran before somebody typed
     * it, not refuse to start.
     */
    private static int configured(StringValueResolver resolver) {
        if (resolver == null) {
            return DEFAULT_CONCURRENCY;
        }
        String value = resolver.resolveStringValue(CONCURRENCY_PROPERTY);
        if (!StringUtils.hasText(value)) {
            return DEFAULT_CONCURRENCY;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_CONCURRENCY;
        }
    }
}
