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

    /** The bounds EQS holds a queue's {@code maxReceiveCount} to. */
    static final int MIN_MAX_RETRIES = 1;
    static final int MAX_MAX_RETRIES = 1000;

    private static final String MAX_RETRIES_PROPERTY = "${euclid.listener.max-retries:0}";

    private ListenerDefaults() {
    }

    /**
     * The delivery attempts a listener's queue should allow before dead-lettering, taking the
     * annotation's own value when it names one and {@code euclid.listener.max-retries} when it
     * does not.
     *
     * <p>Zero means neither said anything, and is kept as zero rather than turned into a default
     * here: the container only changes an existing queue's limit when somebody asked for a limit,
     * and it can only tell that from zero.
     *
     * <p>An annotation outside EQS's bounds is refused at startup - it is written in code, and a
     * value the server will refuse is a mistake better found by whoever wrote it than in a warning
     * at runtime. The property is treated like {@code euclid.listener.concurrency}: a value that is
     * not a number, or out of bounds, counts as unset.
     *
     * @param annotated the annotation's {@code maxRetries}, zero meaning "not said"
     * @param resolver  resolves the property placeholder, or {@code null} outside a context
     * @param listener  how the listener is named in the error for an out-of-bounds annotation
     * @return a limit between 1 and 1000, or zero when none was configured
     */
    static int maxRetries(int annotated, StringValueResolver resolver, String listener) {
        if (annotated != 0) {
            if (annotated < MIN_MAX_RETRIES || annotated > MAX_MAX_RETRIES) {
                throw new IllegalStateException("maxRetries of " + listener + " is " + annotated
                        + "; EQS accepts " + MIN_MAX_RETRIES + " to " + MAX_MAX_RETRIES
                        + ", or leave it at 0 for euclid.listener.max-retries");
            }
            return annotated;
        }
        if (resolver == null) {
            return 0;
        }
        String value = resolver.resolveStringValue(MAX_RETRIES_PROPERTY);
        if (!StringUtils.hasText(value)) {
            return 0;
        }
        try {
            int configured = Integer.parseInt(value.trim());
            return configured >= MIN_MAX_RETRIES && configured <= MAX_MAX_RETRIES ? configured : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
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
