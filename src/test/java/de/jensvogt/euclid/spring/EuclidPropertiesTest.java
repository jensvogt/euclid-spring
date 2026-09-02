package de.jensvogt.euclid.spring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two ways an application learns where euclid is and who it is.
 *
 * <p>One is configured by hand and logs in: {@code euclid.base-url} with a username and password.
 * The other is deployed by euclid itself, which hands it {@code EUCLID_ENDPOINT} and an access key
 * and gives it no password at all - its principal is technical and deliberately cannot log in. The
 * properties have to answer for both without the application knowing which of them it is.
 */
class EuclidPropertiesTest {

    @Test
    void aConfiguredBaseUrlWins() {
        EuclidProperties properties = new EuclidProperties();
        properties.setBaseUrl("https://euclid.example.com:5566");
        properties.setEndpoint("https://localhost:5566");

        // Both names are set only when someone configured one deliberately over what euclid
        // provided, so the deliberate one is the answer.
        assertEquals("https://euclid.example.com:5566", properties.getBaseUrl());
    }

    @Test
    void theManagersEndpointIsUsedWhenNoBaseUrlWasConfigured() {
        EuclidProperties properties = new EuclidProperties();
        properties.setEndpoint("https://localhost:5566");

        assertEquals("https://localhost:5566", properties.getBaseUrl());
    }

    @Test
    void aBlankBaseUrlIsNotAnAnswer() {
        EuclidProperties properties = new EuclidProperties();
        properties.setBaseUrl("  ");
        properties.setEndpoint("https://localhost:5566");

        // An empty value set by a container that exports the variable without a value must not
        // shadow the endpoint that does say something.
        assertEquals("https://localhost:5566", properties.getBaseUrl());
    }

    @Test
    void anAccessKeyIsOnlyAnAccessKeyWhenBothHalvesAreThere() {
        EuclidProperties properties = new EuclidProperties();
        assertFalse(properties.hasAccessKey(), "nothing configured, yet it claims a key");

        properties.setAccessKeyId("AKIAEXAMPLE");
        assertFalse(properties.hasAccessKey(), "half a key is not a key");

        properties.setSecretAccessKey("secret");
        assertTrue(properties.hasAccessKey());
    }

    @Test
    void thePrincipalFallsBackToTheConfiguredUsername() {
        EuclidProperties properties = new EuclidProperties();
        properties.setUsername("alice");
        assertEquals("alice", properties.getUserId(), "a login configuration has no user-id of its own");

        properties.setUserId("app-file-copy");
        assertEquals("app-file-copy", properties.getUserId(), "euclid named the principal, so that is who it is");
    }
}
