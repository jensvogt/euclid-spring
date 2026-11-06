package de.jensvogt.euclid.spring.listener;

import org.junit.jupiter.api.Test;
import org.springframework.util.StringValueResolver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ListenerDefaultsTest {

    private static StringValueResolver property(String value) {
        return placeholder -> placeholder.startsWith("${euclid.listener.max-retries") ? value : placeholder;
    }

    @Test
    void theAnnotationWinsOverTheProperty() {
        assertEquals(7, ListenerDefaults.maxRetries(7, property("5"), "listener"));
    }

    @Test
    void thePropertyAppliesWhenTheAnnotationSaysNothing() {
        assertEquals(5, ListenerDefaults.maxRetries(0, property("5"), "listener"));
    }

    @Test
    void neitherSaysAnythingMeansZero() {
        assertEquals(0, ListenerDefaults.maxRetries(0, property("0"), "listener"));
        assertEquals(0, ListenerDefaults.maxRetries(0, null, "listener"));
    }

    @Test
    void aPropertyThatIsNotUsableCountsAsUnset() {
        assertEquals(0, ListenerDefaults.maxRetries(0, property("lots"), "listener"));
        assertEquals(0, ListenerDefaults.maxRetries(0, property("1001"), "listener"));
    }

    @Test
    void anAnnotationOutOfBoundsIsRefused() {
        assertThrows(IllegalStateException.class, () -> ListenerDefaults.maxRetries(1001, null, "listener"));
        assertThrows(IllegalStateException.class, () -> ListenerDefaults.maxRetries(-1, null, "listener"));
    }
}
