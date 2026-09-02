package de.jensvogt.euclid.spring;

import de.jensvogt.euclid.module.eam.EuclidSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where the session gets its credentials from, and in which order.
 *
 * <p>Three sources, none of which every application has: an access key, the credentials file euclid
 * writes for an application it runs, and a username and password. Only the last one talks to a
 * gateway, so these tests exercise the first two - an application that has neither is the case that
 * used to fail with a null username deep inside the login.
 */
class EuclidSessionSourceTest {

    @TempDir
    Path directory;

    private final EuclidSqsAutoConfiguration configuration = new EuclidSqsAutoConfiguration();

    private Path writeCredentials(String expiresAt) throws IOException {
        return Files.writeString(directory.resolve("credentials"), """
                {"token":"a.bearer.token","expiresAt":"%s","userId":"app-file-copy",
                 "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
                """.formatted(expiresAt));
    }

    private EuclidProperties deployedApplication() throws IOException {
        EuclidProperties properties = new EuclidProperties();
        properties.setEndpoint("https://localhost:5566");
        properties.setCredentialsFile(writeCredentials("2099-01-01T00:00:00Z").toString());
        return properties;
    }

    // Mirrors how Spring wires these two beans together: the reader for the token file exists only
    // in an application that names one, and the session is handed it or nothing accordingly.
    private EuclidSession session(EuclidProperties properties) throws Exception {
        CredentialsFileTokens tokens = properties.hasCredentialsFile()
                ? configuration.euclidCredentialsFileTokens(properties) : null;
        return configuration.euclidSession(properties, TestObjectProvider.of(tokens));
    }

    @Test
    void anApplicationWithOnlyACredentialsFileAuthenticatesWithItsToken() throws Exception {
        EuclidSession session = session(deployedApplication());

        assertEquals("a.bearer.token", session.token());
        assertEquals("app-file-copy", session.userId());
        assertEquals("000000000000", session.accountId());
        assertEquals("eu-central-1", session.region());
        assertEquals("https://localhost:5566", session.baseUrl());
        // A technical principal has no key. Leaving these null is what makes the clients present
        // the bearer token instead of trying to sign with a secret that does not exist.
        assertNull(session.accessKeyId());
        assertNull(session.secretAccessKey());
    }

    @Test
    void anAccessKeyIsPreferredToTheFile() throws Exception {
        EuclidProperties properties = deployedApplication();
        properties.setAccessKeyId("AKIAEXAMPLE");
        properties.setSecretAccessKey("secret");
        properties.setUserId("alice");

        EuclidSession session = session(properties);

        // Both are present only for an application running as a user who may log in, and a signed
        // request is the stronger of the two - it does not expire halfway through the afternoon.
        assertEquals("AKIAEXAMPLE", session.accessKeyId());
        assertEquals("alice", session.userId());
        assertNull(session.token());
    }

    @Test
    void aConfiguredGatewayWinsOverTheOneInTheFile() throws Exception {
        EuclidProperties properties = deployedApplication();
        properties.setBaseUrl("https://euclid.example.com:5566");

        assertEquals("https://euclid.example.com:5566", session(properties).baseUrl());
    }

    @Test
    void theNamespaceAndCaCertificateStillComeFromTheConfiguration() throws Exception {
        EuclidProperties properties = deployedApplication();
        properties.setNamespace("development");
        properties.setCaCertPath("/etc/euclid/ca.pem");

        EuclidSession session = session(properties);

        assertEquals("development", session.nameSpace());
        assertEquals("/etc/euclid/ca.pem", session.caCertPath());
    }

    @Test
    void aDeadTokenIsRefusedAtStartupRatherThanOnEveryCall() throws Exception {
        EuclidProperties properties = deployedApplication();
        properties.setCredentialsFile(writeCredentials(Instant.now().minus(1, ChronoUnit.HOURS).toString()).toString());

        IOException e = assertThrows(IOException.class, () -> session(properties));
        assertTrue(e.getMessage().contains("expired"), e.getMessage());
    }

    @Test
    void anApplicationWithNoCredentialsAtAllSaysSo() {
        EuclidProperties properties = new EuclidProperties();
        properties.setEndpoint("https://localhost:5566");

        // The case that used to reach the gateway client and come back as a NullPointerException
        // about a username, from a stack with nothing in it about euclid configuration.
        IllegalStateException e = assertThrows(IllegalStateException.class,
                                               () -> session(properties));
        assertTrue(e.getMessage().contains("euclid.credentials-file"), e.getMessage());
    }

    @Test
    void anUnreadableFileNamesItselfInTheFailure() throws Exception {
        EuclidProperties properties = deployedApplication();
        properties.setCredentialsFile(directory.resolve("gone").toString());

        IOException e = assertThrows(IOException.class, () -> session(properties));
        assertTrue(e.getMessage().contains("gone"), e.getMessage());
        assertTrue(e.getMessage().contains("euclid.credentials-file"), e.getMessage());
    }
}
