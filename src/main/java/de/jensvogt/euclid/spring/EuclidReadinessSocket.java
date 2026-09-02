package de.jensvogt.euclid.spring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tells the euclid manager that this application is up, by doing the one thing it watches for.
 *
 * <p>An application euclid runs is given a socket path in {@code EUCLID_SOCKET}, and creating that
 * socket <em>is</em> the readiness signal: the manager waits for it to appear and kills the process
 * if it has not within the application's {@code readyTimeoutMs}. An application that starts
 * perfectly well and never binds it is therefore killed and restarted every thirty seconds, having
 * done nothing wrong except not know about a convention it was never told.
 *
 * <p>So this binds it. The listener speaks the minimum that makes the manager's health and metrics
 * calls work - HTTP/1.1 on the socket, a JSON answer to whatever {@code x-euclid-action} asks for -
 * rather than a general dispatch surface: an application that wants to answer real actions can
 * define its own bean and this one steps aside.
 *
 * <p>Absent {@code euclid.socket} it is not created at all, so an application run outside euclid -
 * from an IDE, from a jar, in a test - is unaffected.
 */
public class EuclidReadinessSocket implements SmartLifecycle {

    private static final Log logger = LogFactory.getLog(EuclidReadinessSocket.class);

    private static final String RESPONSE = """
            HTTP/1.1 200 OK\r
            Content-Type: application/json\r
            Content-Length: %d\r
            Connection: close\r
            \r
            %s""";

    private final Path socketPath;

    private volatile ServerSocketChannel channel;
    private volatile Thread acceptor;
    private volatile boolean running;

    /**
     * @param socketPath the path from {@code EUCLID_SOCKET}
     */
    public EuclidReadinessSocket(String socketPath) {
        this.socketPath = Path.of(socketPath);
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        try {
            // A path left behind by a process the manager killed is not a listener - binding onto
            // it would fail with "address already in use" for a socket nobody is serving.
            Files.deleteIfExists(socketPath);
            if (socketPath.getParent() != null) {
                Files.createDirectories(socketPath.getParent());
            }

            ServerSocketChannel server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            server.bind(UnixDomainSocketAddress.of(socketPath));
            channel = server;
            running = true;

            acceptor = new Thread(this::acceptLoop, "euclid-readiness");
            acceptor.setDaemon(true);
            acceptor.start();

            logger.info("Listening on " + socketPath + " - euclid reads that as this application being ready");

        } catch (IOException e) {
            // Not fatal: an application that cannot bind it is killed by the manager for being
            // unready, and saying why here is more use than refusing to start at all.
            logger.error("Could not create the readiness socket at " + socketPath
                    + "; euclid will treat this application as never having become ready", e);
        }
    }

    @Override
    public void stop() {
        running = false;
        ServerSocketChannel server = channel;
        channel = null;
        if (server != null) {
            try {
                server.close();
            } catch (IOException e) {
                logger.debug("Could not close the readiness socket", e);
            }
        }
        if (acceptor != null) {
            acceptor.interrupt();
            acceptor = null;
        }
        try {
            Files.deleteIfExists(socketPath);
        } catch (IOException e) {
            logger.debug("Could not remove the readiness socket file", e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Started late and stopped early: the socket says "ready", so it should not appear before the
     * listeners and clients behind it exist.
     *
     * @return the lifecycle phase
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void acceptLoop() {
        while (running) {
            ServerSocketChannel server = channel;
            if (server == null) {
                return;
            }
            try (SocketChannel connection = server.accept()) {
                answer(connection);
            } catch (IOException e) {
                if (running) {
                    logger.debug("Readiness socket accept failed", e);
                }
                return;
            }
        }
    }

    private void answer(SocketChannel connection) throws IOException {
        // The request is read and discarded: the manager's health check is a request being
        // answered at all, and this listener has nothing to say about its contents.
        ByteBuffer request = ByteBuffer.allocate(4096);
        connection.read(request);

        String body = "{\"status\":\"UP\"}";
        String response = RESPONSE.formatted(body.length(), body);
        connection.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
    }
}
