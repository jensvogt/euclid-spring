package de.jensvogt.euclid.spring;

import org.junit.jupiter.api.Test;

import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The manager decides an application is ready by watching for its socket, and kills it when the
 * socket does not appear - so these are the two things that matter: that the socket exists while
 * the application runs, and that something on the other end answers.
 */
class EuclidReadinessSocketTest {

    @Test
    void theSocketExistsAndAnswersWhileTheApplicationRuns() throws Exception {
        Path path = Files.createTempDirectory("euclid-readiness").resolve("app.sock");
        EuclidReadinessSocket socket = new EuclidReadinessSocket(path.toString());

        socket.start();
        try {
            assertTrue(Files.exists(path), "the manager watches for this file and would kill the application");

            try (SocketChannel client = SocketChannel.open(StandardProtocolFamily.UNIX)) {
                client.connect(UnixDomainSocketAddress.of(path));
                client.write(ByteBuffer.wrap("POST / HTTP/1.1\r\nx-euclid-action: get-metrics\r\n\r\n"
                                                     .getBytes(StandardCharsets.UTF_8)));

                ByteBuffer response = ByteBuffer.allocate(512);
                client.read(response);
                String text = new String(response.array(), 0, response.position(), StandardCharsets.UTF_8);

                assertTrue(text.startsWith("HTTP/1.1 200"), "the manager's health check got: " + text);
                assertTrue(text.contains("\"status\":\"UP\""), "no JSON body: " + text);
            }
        } finally {
            socket.stop();
        }

        // Left behind, the path is not a listener but still looks like one - the next start would
        // fail to bind onto it.
        assertFalse(Files.exists(path), "the socket file outlived the application");
    }

    @Test
    void aPathLeftByAKilledProcessIsRebound() throws Exception {
        Path path = Files.createTempDirectory("euclid-readiness").resolve("app.sock");
        Files.createFile(path);

        EuclidReadinessSocket socket = new EuclidReadinessSocket(path.toString());
        socket.start();
        try {
            // Exactly the state an application killed for being unready leaves behind, and the
            // state its restart has to survive.
            assertTrue(socket.isRunning(), "a stale socket file stopped the application from becoming ready");
        } finally {
            socket.stop();
        }
    }
}
