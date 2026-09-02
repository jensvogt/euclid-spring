package de.jensvogt.euclid.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for connecting to an Euclid server, bound from {@code euclid.*} properties.
 */
@ConfigurationProperties(prefix = "euclid")
public class EuclidProperties {

    /**
     * Base URL of the Euclid server, e.g. {@code https://euclid.example.com}.
     */
    private String baseUrl;

    private String username;

    private String password;

    /**
     * Namespace to make active for the session, or {@code null} to leave it unscoped.
     *
     * <p>Applied at login, so every namespace-scoped call the auto-configured clients make is
     * restricted to it. Worth setting whenever more than one namespace exists: an unscoped session
     * resolving a bucket or queue by name has nothing to disambiguate two of the same name with.
     */
    private String namespace;

    /**
     * Additional PEM CA certificate trusted for TLS connections to {@code baseUrl}, or
     * {@code null} to trust only the system store.
     */
    private String caCertPath;

    /**
     * Access key ID an application deployed by euclid signs its requests with, or {@code null}
     * when the session is established by logging in.
     *
     * <p>Bound from {@code EUCLID_ACCESS_KEY_ID}, which the euclid manager puts in every
     * application's environment. An application euclid runs has no password to log in with - its
     * principal is technical and deliberately cannot - so the key is how it authenticates.
     */
    private String accessKeyId;

    /**
     * Secret half of {@link #accessKeyId}, bound from {@code EUCLID_SECRET_ACCESS_KEY}.
     */
    private String secretAccessKey;

    /**
     * File the euclid manager writes this application's bearer token to, bound from
     * {@code EUCLID_CREDENTIALS_FILE}.
     *
     * <p>How an application running as a technical principal authenticates. Such a principal is
     * given no access key at all - only a named user who may log in gets {@link #accessKeyId} in
     * its environment - so without this there is nothing to present and nothing to log in with.
     *
     * @see ApplicationCredentials
     */
    private String credentialsFile;

    /**
     * Endpoint under the name the euclid manager uses, {@code EUCLID_ENDPOINT}.
     *
     * <p>Only a fallback for {@link #baseUrl}: an application configured by hand sets
     * {@code euclid.base-url}, and one deployed by euclid is given {@code EUCLID_ENDPOINT}. Both
     * name the same gateway, and {@link #getBaseUrl()} answers with whichever was provided.
     */
    private String endpoint;

    /**
     * Account the session acts in, bound from {@code EUCLID_ACCOUNT_ID}.
     *
     * <p>Only needed when there is no login to learn it from - an application euclid deployed is
     * told which account it belongs to rather than discovering it.
     */
    /**
     * Unix socket path this application must listen on to be considered ready, bound from
     * {@code EUCLID_SOCKET}.
     *
     * <p>Set by the euclid manager for an application it runs, and by nothing else - so its
     * presence is also how the starter knows it is running inside euclid rather than from an IDE
     * or a plain {@code java -jar}.
     */
    private String socket;

    private String accountId;

    /**
     * Region the session acts in, bound from {@code EUCLID_REGION}. Same reasoning as
     * {@link #accountId}.
     */
    private String region;

    /**
     * The principal the application runs as, bound from {@code EUCLID_USER_ID}. Falls back to
     * {@link #username} for a configuration that logs in.
     */
    private String userId;

    /**
     * The gateway to talk to: {@code euclid.base-url} if it was configured, otherwise the
     * {@code EUCLID_ENDPOINT} the manager hands a deployed application.
     *
     * @return the endpoint, or {@code null} if neither was set
     */
    public String getBaseUrl() {
        return baseUrl == null || baseUrl.isBlank() ? endpoint : baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getSocket() {
        return socket;
    }

    public void setSocket(String socket) {
        this.socket = socket;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * The principal to act as: {@code euclid.user-id} when euclid deployed this application,
     * otherwise the configured username.
     *
     * @return the user ID, or {@code null} if neither was set
     */
    public String getUserId() {
        return userId == null || userId.isBlank() ? username : userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Whether this configuration names an access key rather than a password.
     *
     * @return true when both halves of a key are set
     */
    public boolean hasAccessKey() {
        return accessKeyId != null && !accessKeyId.isBlank() && secretAccessKey != null && !secretAccessKey.isBlank();
    }

    public String getCredentialsFile() {
        return credentialsFile;
    }

    public void setCredentialsFile(String credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    /**
     * Whether this configuration names a credentials file to take a bearer token from.
     *
     * @return true when {@code euclid.credentials-file} is set
     */
    public boolean hasCredentialsFile() {
        return credentialsFile != null && !credentialsFile.isBlank();
    }

    public String getCaCertPath() {
        return caCertPath;
    }

    public void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }
}
