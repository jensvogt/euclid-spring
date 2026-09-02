package de.jensvogt.euclid.spring;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

/**
 * An {@link ObjectProvider} over one value, or over nothing - which is how the auto-configuration
 * is told whether this application has a credentials file, and so what the tests below have to be
 * able to say.
 *
 * @param <T> the type provided
 */
final class TestObjectProvider<T> implements ObjectProvider<T> {

    private final T value;

    private TestObjectProvider(T value) {
        this.value = value;
    }

    /**
     * A provider of {@code value}, or of nothing when it is {@code null}.
     *
     * @param value the value to provide, or {@code null} for a provider with no bean behind it
     * @param <T>   the type provided
     * @return the provider
     */
    static <T> ObjectProvider<T> of(T value) {
        return new TestObjectProvider<>(value);
    }

    @Override
    public T getObject() {
        if (value == null) {
            throw new NoSuchBeanDefinitionException("nothing provided");
        }
        return value;
    }

    @Override
    public T getObject(Object... args) {
        return getObject();
    }

    @Override
    public T getIfAvailable() {
        return value;
    }

    @Override
    public T getIfUnique() {
        return value;
    }
}
