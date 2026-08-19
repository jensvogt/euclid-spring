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
     * Additional PEM CA certificate trusted for TLS connections to {@code baseUrl}, or
     * {@code null} to trust only the system store.
     */
    private String caCertPath;

    public String getBaseUrl() {
        return baseUrl;
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

    public String getCaCertPath() {
        return caCertPath;
    }

    public void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }
}
