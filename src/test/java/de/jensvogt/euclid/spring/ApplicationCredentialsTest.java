package de.jensvogt.euclid.spring;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reading the credentials euclid writes for an application it runs.
 *
 * <p>The manager owns the format, so these tests use the file as it actually writes it - including
 * a field this does not know about, since a manager newer than the starter must not stop the
 * application from starting.
 */
class ApplicationCredentialsTest {

    @TempDir
    Path directory;

    private Path write(String json) throws IOException {
        return Files.writeString(directory.resolve("credentials"), json);
    }

    @Test
    void readsTheFileTheManagerWrites() throws IOException {
        Path file = write("""
                {"token":"eyJhbGciOiJIUzI1NiJ9.payload.signature",
                 "expiresAt":"2099-01-01T00:00:00Z",
                 "userId":"app-file-copy",
                 "accountId":"000000000000",
                 "region":"eu-central-1",
                 "endpoint":"https://localhost:5566",
                 "somethingAddedLater":"ignored"}
                """);

        ApplicationCredentials credentials = ApplicationCredentials.read(file);

        assertEquals("eyJhbGciOiJIUzI1NiJ9.payload.signature", credentials.token());
        assertEquals("app-file-copy", credentials.userId());
        assertEquals("000000000000", credentials.accountId());
        assertEquals("eu-central-1", credentials.region());
        assertEquals("https://localhost:5566", credentials.endpoint());
        assertFalse(credentials.isExpired());
    }

    @Test
    void aFileWithoutATokenIsNoUse() throws IOException {
        Path file = write("{\"userId\":\"app-file-copy\"}");

        // Better to say so here than to start and have every call come back unauthorized.
        IOException e = assertThrows(IOException.class, () -> ApplicationCredentials.read(file));
        assertTrue(e.getMessage().contains("No token"), e.getMessage());
    }

    @Test
    void aMissingFileSaysWhichFile() {
        Path file = directory.resolve("absent");

        assertThrows(IOException.class, () -> ApplicationCredentials.read(file));
    }

    @Test
    void anElapsedExpiryIsExpired() throws IOException {
        Path file = write("{\"token\":\"t\",\"expiresAt\":\""
                                  + Instant.now().minus(1, ChronoUnit.MINUTES) + "\"}");

        assertTrue(ApplicationCredentials.read(file).isExpired());
    }

    @Test
    void anExpiryThisCannotReadIsNotTakenAsExpired() throws IOException {
        // A timestamp in a shape this does not parse, and one the manager left out entirely. The
        // token may well be fine; refusing it would turn a cosmetic mismatch into an outage, and
        // the gateway is the one that decides anyway.
        assertFalse(ApplicationCredentials.read(write("{\"token\":\"t\",\"expiresAt\":\"tomorrow\"}")).isExpired());
        assertFalse(ApplicationCredentials.read(write("{\"token\":\"t\"}")).isExpired());
    }
}
