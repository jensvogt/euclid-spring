package de.jensvogt.euclid.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import de.jensvogt.euclid.module.eam.EuclidSession;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every module client the starter is supposed to offer, and one in particular.
 *
 * <p>{@code EuclidEap} was missing for months without anything saying so.
 * {@code EuclidListenerContainer} asks for one through an {@code ObjectProvider} to report this
 * instance's load to, and with no bean to hand it the call sits behind a null check - so the report
 * was skipped, no exception was thrown, and nothing was logged. Every application pushed its metrics
 * to EMO on the same tick and reported to the autoscaler never, which from outside is
 * indistinguishable from load reporting that was never built.
 *
 * <p>What it cost is worth stating, because it is why this test exists rather than a comment: the
 * manager fell back to EMO's five-minute buckets for every scaling decision, and never learned the
 * live handler count - the figure that stops it scaling down an instance in the middle of a message.
 * Instances were stopped mid-parse, their JDBC sockets closed by the interrupt, and the work
 * redelivered.
 *
 * <p>A bean that is only ever consumed through an {@code ObjectProvider} has no other way of being
 * missed: nothing fails to start, and the feature that needed it simply does not happen.
 */
class ModuleBeansTest {

    @TempDir
    Path directory;

    private final EuclidSqsAutoConfiguration configuration = new EuclidSqsAutoConfiguration();

    private EuclidSession session() throws Exception {
        Path credentials = Files.writeString(directory.resolve("credentials"), """
                {"token":"a.bearer.token","expiresAt":"2099-01-01T00:00:00Z","userId":"app-parser",
                 "accountId":"000000000000","region":"eu-central-1","endpoint":"https://localhost:5566"}
                """);

        EuclidProperties properties = new EuclidProperties();
        properties.setEndpoint("https://localhost:5566");
        properties.setCredentialsFile(credentials.toString());
        return configuration.euclidSession(properties,
                TestObjectProvider.of(configuration.euclidCredentialsFileTokens(properties)));
    }

    @Test
    void theApplicationModuleClientIsOffered() throws Exception {
        // The one that was missing. Load reporting is the only thing that asks for it, and it asks
        // in a way that cannot complain.
        assertNotNull(configuration.euclidEap(session(), TestObjectProvider.of(null)));
    }

    @Test
    void everyOtherModuleClientIsStillOffered() throws Exception {
        // Pinned together so the next one to go missing is caught by the same reasoning rather than
        // by somebody noticing a feature quietly not happening.
        EuclidSession session = session();

        assertNotNull(configuration.euclidSqs(session, TestObjectProvider.of(null)));
        assertNotNull(configuration.euclidEsm(session, TestObjectProvider.of(null)));
        assertNotNull(configuration.euclidEns(session, TestObjectProvider.of(null)));
        assertNotNull(configuration.euclidEmo(session, TestObjectProvider.of(null)));
        assertNotNull(configuration.euclidEes(session, TestObjectProvider.of(null)));
    }
}
