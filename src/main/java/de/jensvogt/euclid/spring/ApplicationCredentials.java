package de.jensvogt.euclid.spring;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * The credentials the euclid manager writes for an application it runs, read from the file named
 * by {@code EUCLID_CREDENTIALS_FILE}.
 *
 * <p>An application that runs as a technical principal - one euclid created for it, rather than a
 * user somebody named - is deliberately given no access key: its long-lived secret stays in EAM
 * and the process holds nothing but a bearer token that expires. That token cannot travel in the
 * environment, because an environment cannot be rewritten after {@code exec()} and the manager
 * replaces these while the process runs; hence a file, the same arrangement AWS uses for container
 * credentials.
 *
 * <p>The file is JSON and is installed by an atomic rename, so a reader either sees the previous
 * contents or the new ones, never half of either:
 *
 * <pre>
 * {"token":"…","expiresAt":"2026-09-02T12:00:00Z","userId":"app-file-copy",
 *  "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
 * </pre>
 *
 * @param token      bearer token for the identity the application runs as
 * @param expiresAt  when that token stops being accepted
 * @param userId     the principal the token names
 * @param accountId  account the application belongs to
 * @param region     region the application belongs to
 * @param endpoint   gateway the token is good for
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ApplicationCredentials(String token, String expiresAt, String userId, String accountId,
                                     String region, String endpoint) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Reads and parses the credentials file.
     *
     * @param path path to the file, as given in {@code EUCLID_CREDENTIALS_FILE}
     * @return the credentials it holds
     * @throws IOException if the file cannot be read or does not parse
     */
    public static ApplicationCredentials read(Path path) throws IOException {
        final ApplicationCredentials credentials = MAPPER.readValue(Files.readAllBytes(path), ApplicationCredentials.class);
        if (credentials.token() == null || credentials.token().isBlank()) {
            throw new IOException("No token in credentials file: " + path);
        }
        return credentials;
    }

    /**
     * Whether the token has passed its expiry.
     *
     * <p>Unparseable or absent expiry counts as not expired: the manager refreshes the file well
     * before the token runs out, and refusing to start over a timestamp this cannot read would
     * turn a cosmetic problem into an outage.
     *
     * @return true if the token is known to have expired
     */
    public boolean isExpired() {
        if (expiresAt == null || expiresAt.isBlank()) {
            return false;
        }
        try {
            return Instant.parse(expiresAt).isBefore(Instant.now());
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
