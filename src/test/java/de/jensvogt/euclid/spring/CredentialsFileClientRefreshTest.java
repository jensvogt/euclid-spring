package de.jensvogt.euclid.spring;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import de.jensvogt.euclid.module.eam.EuclidSession;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * That the auto-configured clients, not just the reader behind them, follow the credentials file.
 *
 * <p>The wiring is the part worth proving: a client is built once, at startup, from a session that
 * holds whatever token was in the file then. Unless each client is also handed the reader, the
 * refresh happens where nobody is listening. So this asks a real client to make a real request,
 * rewrites the file the way euclid does, and asks it again.
 */
class CredentialsFileClientRefreshTest {

    @TempDir
    Path directory;

    private final EuclidSqsAutoConfiguration configuration = new EuclidSqsAutoConfiguration();

    private final List<String> authorizations = new ArrayList<>();

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private Path writeCredentials(String token, Instant modified) throws IOException {
        Path file = Files.writeString(directory.resolve("credentials"), """
                {"token":"%s","expiresAt":"2099-01-01T00:00:00Z","userId":"app-file-copy",
                 "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
                """.formatted(token));
        Files.setLastModifiedTime(file, FileTime.from(modified));
        return file;
    }

    @Test
    void aClientBuiltAtStartupFollowsTheFileForTheRestOfItsLife() throws Exception {
        startServer();
        Path file = writeCredentials("token-one", Instant.now());

        EuclidProperties properties = new EuclidProperties();
        properties.setBaseUrl(baseUrl());
        properties.setCredentialsFile(file.toString());

        CredentialsFileTokens tokens = configuration.euclidCredentialsFileTokens(properties);
        EuclidSession session = configuration.euclidSession(properties, TestObjectProvider.of(tokens));
        EuclidEqs eqs = configuration.euclidSqs(session, TestObjectProvider.of(tokens));

        eqs.listQueues();
        writeCredentials("token-two", Instant.now().plus(30, ChronoUnit.MINUTES));
        eqs.listQueues();

        assertEquals(List.of("Bearer token-one", "Bearer token-two"), authorizations);
    }

    @Test
    void anApplicationWithoutACredentialsFileKeepsTheTokenItLoggedInWith() throws Exception {
        startServer();

        EuclidProperties properties = new EuclidProperties();
        properties.setBaseUrl(baseUrl());
        EuclidSession session = new EuclidSession("login-token", "alice", "000000000000", "eu-central-1",
                                                  null, null, false, null, baseUrl(), null, null);

        // No reader to hand over, and nothing to follow: the session from a login holds a token
        // this starter has no way of renewing, and the client has to keep using it.
        EuclidEqs eqs = configuration.euclidSqs(session, TestObjectProvider.of(null));
        eqs.listQueues();

        assertEquals(List.of("Bearer login-token"), authorizations);
    }

    private void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            respond(exchange);
        });
        server.start();
    }

    private String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange) throws IOException {
        byte[] body = "{\"queues\":[],\"total\":0}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
