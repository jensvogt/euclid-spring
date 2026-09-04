package de.jensvogt.euclid.spring;

import de.jensvogt.euclid.Euclid;
import de.jensvogt.euclid.auth.TokenRefreshable;
import de.jensvogt.euclid.module.eam.EuclidEam;
import de.jensvogt.euclid.module.eam.EuclidSession;
import de.jensvogt.euclid.module.ees.EuclidEes;
import de.jensvogt.euclid.module.emo.EuclidEmo;
import de.jensvogt.euclid.module.ens.EuclidEns;
import de.jensvogt.euclid.module.eqs.EuclidEqs;
import de.jensvogt.euclid.module.esm.EuclidEsm;
import de.jensvogt.euclid.spring.listener.BucketListenerBeanPostProcessor;
import de.jensvogt.euclid.spring.listener.EuclidListenerContainer;
import de.jensvogt.euclid.spring.listener.QueueListenerBeanPostProcessor;
import de.jensvogt.euclid.spring.listener.TopicListenerBeanPostProcessor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Autoconfigures {@link EuclidEqs}, {@link EuclidEsm}, {@link EuclidEes} and {@link EuclidEns}
 * clients from {@code euclid.*} properties, plus the infrastructure backing {@code @QueueListener},
 * {@code @TopicListener} and {@code @BucketListener}.
 */
@AutoConfiguration
@EnableConfigurationProperties(EuclidProperties.class)
// Either name for the gateway is enough to configure euclid: an application configured by hand
// sets euclid.base-url, and one euclid deployed itself is given EUCLID_ENDPOINT.
@Conditional(EuclidSqsAutoConfiguration.OnEuclidEndpoint.class)
public class EuclidSqsAutoConfiguration {

    /**
     * Matches when a gateway is named, under either of the two property names that name one.
     */
    static class OnEuclidEndpoint implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return StringUtils.hasText(context.getEnvironment().getProperty("euclid.base-url"))
                    || StringUtils.hasText(context.getEnvironment().getProperty("euclid.endpoint"));
        }
    }

    /**
     * Establishes the session once, so every module client below is derived from the same one.
     *
     * <p>Three ways to authenticate, tried in that order of directness - an access key, the
     * credentials file euclid writes, then a login. The first two need no round trip; only the
     * last one talks to the gateway.
     * <p>
     * The token file euclid writes for an application it deployed, read now and re-read as euclid
     * replaces it. Absent from applications that authenticate some other way.
     *
     * @param properties the configuration naming the file
     * @return a reader for that file, which every client below takes its token from
     * @throws IOException if the file cannot be read or does not parse
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "euclid", name = "credentials-file")
    public CredentialsFileTokens euclidCredentialsFileTokens(EuclidProperties properties) throws IOException {
        final Path path = Path.of(properties.getCredentialsFile());
        try {
            return new CredentialsFileTokens(path);
        } catch (IOException e) {
            throw new IOException("Cannot read the euclid credentials file " + path
                                          + " named by euclid.credentials-file: " + e.getMessage(), e);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidSession euclidSession(EuclidProperties properties,
                                       ObjectProvider<CredentialsFileTokens> credentialsFile) throws IOException, InterruptedException {
        // An application euclid runs authenticates with the access key its principal was given,
        // not with a password: that principal is technical and deliberately cannot log in. So when
        // a key is configured - EUCLID_ACCESS_KEY_ID and EUCLID_SECRET_ACCESS_KEY, which the
        // manager puts in the environment of an application that runs as a user who may log in -
        // the session is built from it directly and no login is attempted. Every client signs its
        // requests with the key anyway; the login exists only to obtain one.
        if (properties.hasAccessKey()) {
            return new EuclidSession(null, properties.getUserId(), properties.getAccountId(),
                                     properties.getRegion(), properties.getAccessKeyId(), properties.getSecretAccessKey(),
                                     false, null, properties.getBaseUrl(), properties.getCaCertPath(),
                                     properties.getNamespace());
        }

        // An application running as a technical principal gets no key - the principal has no
        // password either, so there is nothing to log in with - and is instead handed a bearer
        // token in the file named by EUCLID_CREDENTIALS_FILE. The file also carries the identity
        // the token names, which is authoritative: it is what the gateway will see, whatever the
        // environment happens to say.
        if (properties.hasCredentialsFile()) {
            return fromCredentialsFile(properties, credentialsFile.getObject());
        }

        // Nothing left to authenticate with. Said here rather than let through: login() fails on a
        // null username with a NullPointerException from inside the client, several frames away
        // from the configuration that is actually missing.
        if (!StringUtils.hasText(properties.getUsername())) {
            throw new IllegalStateException(
                    "No euclid credentials: set euclid.username and euclid.password, or an access key"
                            + " (euclid.access-key-id, euclid.secret-access-key), or euclid.credentials-file."
                            + " An application euclid deploys is given one of the latter two in its"
                            + " environment - if this is one, EUCLID_ACCESS_KEY_ID and EUCLID_CREDENTIALS_FILE"
                            + " are both absent from it.");
        }

        EuclidEam euclidEam = Euclid.forServer(properties.getBaseUrl())
                .access()
                .credentials(properties.getUsername(), properties.getPassword());
        if (properties.getCaCertPath() != null) {
            euclidEam.caCertPath(properties.getCaCertPath());
        }
        if (StringUtils.hasText(properties.getNamespace())) {
            euclidEam.namespace(properties.getNamespace());
        }
        return euclidEam.login();
    }

    /**
     * Builds a session from the token euclid left in {@code EUCLID_CREDENTIALS_FILE}.
     *
     * <p>The file's own account, region and user win over the environment where it has them: both
     * are written by the same manager, but the file is rewritten as the token is renewed, so it is
     * the fresher of the two - and it says who the token will actually be taken for. The gateway
     * is the other way round, since {@code euclid.base-url} may have been set deliberately to
     * reach the same server by another route; the file only supplies one if nothing else did.
     *
     * <p>The token in this session is the one the file holds now, which is a snapshot: the clients
     * below take theirs from {@code credentialsFile} on each request instead, so they keep working
     * once euclid has replaced this one.
     *
     * @param properties     the configuration naming the file
     * @param credentialsFile the file's current contents, re-read as euclid rewrites it
     * @return a session authenticating as the application's principal
     * @throws IOException if the token has already expired
     */
    private static EuclidSession fromCredentialsFile(EuclidProperties properties,
                                                     CredentialsFileTokens credentialsFile) throws IOException {
        final Path path = credentialsFile.path();
        final ApplicationCredentials credentials = credentialsFile.current();
        if (credentials.isExpired()) {
            // Only reachable if nothing renewed the file, since the manager replaces it at half
            // the token's lifetime. Starting anyway would mean every call failing as unauthorized
            // with nothing to point at the cause.
            throw new IOException("The euclid credentials file " + path + " holds a token that expired at "
                                          + credentials.expiresAt() + "; is the euclid manager still running?");
        }
        return new EuclidSession(credentials.token(), or(credentials.userId(), properties.getUserId()),
                                 or(credentials.accountId(), properties.getAccountId()),
                                 or(credentials.region(), properties.getRegion()), null, null,
                                 false, null, or(properties.getBaseUrl(), credentials.endpoint()),
                                 properties.getCaCertPath(),
                                 // An explicitly configured namespace wins, as it does for every
                                 // other field here; without one the application inherits the
                                 // namespace it was deployed into. That inheritance is what lets
                                 // it name a queue or topic rather than spell out a full ERN, and
                                 // it is why an application needs no scope configuration of its
                                 // own at all.
                                 or(properties.getNamespace(), credentials.nameSpace()));
    }

    /**
     * The first of two values that was actually set.
     *
     * @param preferred the value to use if it has text
     * @param fallback  the value to use otherwise
     * @return whichever applies
     */
    private static String or(String preferred, String fallback) {
        return StringUtils.hasText(preferred) ? preferred : fallback;
    }

    /**
     * Points a client at the credentials file, where there is one, so it asks for a token per
     * request instead of keeping the one it was built with. A no-op for every other way of
     * authenticating: a session established by login or by access key has nothing to re-read.
     *
     * @param client          the client to configure
     * @param credentialsFile the token file, if this application has one
     * @param <T>             the client type
     * @return the same client
     */
    private static <T extends TokenRefreshable> T refreshing(T client, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        credentialsFile.ifAvailable(client::token);
        return client;
    }

    /**
     * Answers the euclid manager's readiness check, when euclid is what started this application.
     *
     * <p>Creating the socket named by {@code EUCLID_SOCKET} is the readiness signal: the manager
     * waits for it and kills the process if it never appears, so an application that starts
     * perfectly well but does not bind it is killed and restarted for as long as it lives. Only
     * created when that variable is set - which euclid sets and nothing else does - so an
     * application run from an IDE or a plain jar is unaffected. A caller that wants to serve real
     * actions on that socket defines its own bean and this one steps aside.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "euclid", name = "socket")
    public EuclidReadinessSocket euclidReadinessSocket(EuclidProperties properties) {
        return new EuclidReadinessSocket(properties.getSocket());
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEqs euclidSqs(EuclidSession euclidSession, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        return refreshing(euclidSession.eqs(), credentialsFile);
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEsm euclidEsm(EuclidSession euclidSession, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        return refreshing(euclidSession.esm(), credentialsFile);
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEes euclidEes(EuclidSession euclidSession, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        return refreshing(euclidSession.ees(), credentialsFile);
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEns euclidEns(EuclidSession euclidSession, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        return refreshing(euclidSession.ens(), credentialsFile);
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidEmo euclidEmo(EuclidSession euclidSession, ObjectProvider<CredentialsFileTokens> credentialsFile) {
        return refreshing(euclidSession.emo(), credentialsFile);
    }

    @Bean
    @ConditionalOnMissingBean
    public EuclidListenerContainer euclidListenerContainer(ObjectProvider<EuclidEqs> euclidSqsProvider,
                                                             ObjectProvider<EuclidEsm> euclidEsmProvider,
                                                             ObjectProvider<EuclidEns> euclidEnsProvider,
                                                           ObjectProvider<EuclidEmo> euclidEmoProvider,
                                                             ObjectProvider<JsonMapper> objectMapperProvider) {
        return new EuclidListenerContainer(euclidSqsProvider, euclidEsmProvider, euclidEnsProvider, euclidEmoProvider, objectMapperProvider);
    }

    // A bean post processor is instantiated ahead of the ordinary singletons, so a @Bean method
    // declaring one is static: an instance method would need this configuration class built that
    // early too, before it can be post-processed itself. Each takes the container as a provider
    // for the same reason - see the post processors' constructors.
    @Bean
    public static QueueListenerBeanPostProcessor queueListenerBeanPostProcessor(
            ObjectProvider<EuclidListenerContainer> container) {
        return new QueueListenerBeanPostProcessor(container);
    }

    @Bean
    public static TopicListenerBeanPostProcessor topicListenerBeanPostProcessor(
            ObjectProvider<EuclidListenerContainer> container) {
        return new TopicListenerBeanPostProcessor(container);
    }

    @Bean
    public static BucketListenerBeanPostProcessor bucketListenerBeanPostProcessor(
            ObjectProvider<EuclidListenerContainer> container) {
        return new BucketListenerBeanPostProcessor(container);
    }
}
