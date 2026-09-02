package de.jensvogt.euclid.spring;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Following the credentials file as euclid rewrites it.
 *
 * <p>The manager replaces the file at half the token's lifetime - hourly tokens, a new file every
 * thirty minutes - so an application that read it once would fail partway through its second hour.
 * What is asserted here is that the next request after a rewrite carries the new token, and that
 * nothing worse than staleness happens if the file becomes unreadable.
 */
class CredentialsFileTokensTest {

    @TempDir
    Path directory;

    private Path file;

    private Path write(String token, Instant modified) throws IOException {
        file = Files.writeString(directory.resolve("credentials"), """
                {"token":"%s","expiresAt":"2099-01-01T00:00:00Z","userId":"app-file-copy",
                 "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
                """.formatted(token));
        // Set explicitly rather than relying on the clock: two writes in the same millisecond are
        // ordinary in a test and would leave the timestamps equal, which is not what euclid's
        // rewrite thirty minutes later looks like.
        Files.setLastModifiedTime(file, FileTime.from(modified));
        return file;
    }

    @Test
    void theNextRequestAfterARewriteCarriesTheNewToken() throws IOException {
        CredentialsFileTokens tokens = new CredentialsFileTokens(write("token-one", Instant.now()));
        assertEquals("token-one", tokens.get());

        write("token-two", Instant.now().plus(30, ChronoUnit.MINUTES));

        assertEquals("token-two", tokens.get());
    }

    @Test
    void anUntouchedFileIsNotReadAgain() throws IOException {
        CredentialsFileTokens tokens = new CredentialsFileTokens(write("token-one", Instant.now()));

        // Same instance back, so the file was not parsed a second time - this is consulted on
        // every request that goes out, and euclid changes it twice an hour.
        assertSame(tokens.current(), tokens.current());
    }

    @Test
    void aFileThatBecomesUnreadableLeavesTheLastTokenInPlace() throws IOException {
        CredentialsFileTokens tokens = new CredentialsFileTokens(write("token-one", Instant.now()));
        Files.delete(file);

        // The token may still have most of its life left, and failing every call over a file that
        // has gone missing would take an application down that would otherwise have kept running
        // until somebody read the warning.
        assertEquals("token-one", tokens.get());
    }

    @Test
    void garbageInPlaceOfTheFileLeavesTheLastTokenInPlace() throws IOException {
        CredentialsFileTokens tokens = new CredentialsFileTokens(write("token-one", Instant.now()));
        Files.writeString(file, "not json at all");
        Files.setLastModifiedTime(file, FileTime.from(Instant.now().plus(30, ChronoUnit.MINUTES)));

        assertEquals("token-one", tokens.get());
    }

    @Test
    void aFileThatCannotBeReadAtAllIsRefusedUpFront() {
        // Startup is where a missing file has to be reported: it is a configuration error, and
        // there is no previous token to fall back on.
        assertThrows(IOException.class, () -> new CredentialsFileTokens(directory.resolve("absent")));
    }
}
