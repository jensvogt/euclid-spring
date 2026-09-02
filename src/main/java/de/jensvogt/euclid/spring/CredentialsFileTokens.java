package de.jensvogt.euclid.spring;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.function.Supplier;

/**
 * The current bearer token from the credentials file euclid writes, re-read as euclid replaces it.
 *
 * <p>A token is good for an hour by default and euclid rewrites the file at half that, so an
 * application that read the file once at startup and kept the string would begin failing partway
 * through its second hour. Handing this to the clients instead - they take their token from a
 * supplier, per request - is what lets a process outlive the token it started with.
 *
 * <p>Freshness is decided by the file's modification time rather than by a timer: euclid installs
 * a new file by renaming one into place, so the timestamp changes exactly when the token does, and
 * a stat call per request costs nothing next to the request itself. A re-read that fails leaves
 * the previous token in place - it may still be valid, and an expired token that the gateway
 * rejects is a better answer than an exception from inside an unrelated call.
 *
 * @see ApplicationCredentials
 */
public final class CredentialsFileTokens implements Supplier<String> {

    private static final Log logger = LogFactory.getLog(CredentialsFileTokens.class);

    private final Path path;

    private volatile ApplicationCredentials credentials;

    private volatile FileTime lastModified;

    /**
     * The file version a failure was last reported for, so an unreadable file is complained about
     * once rather than on every request that goes out while it stays that way.
     */
    private FileTime warnedFor;

    /**
     * Reads the file for the first time, so a configuration that names an unreadable one fails
     * while the application context is starting rather than on its first request.
     *
     * @param path the credentials file, as named by {@code EUCLID_CREDENTIALS_FILE}
     * @throws IOException if the file cannot be read or does not parse
     */
    public CredentialsFileTokens(Path path) throws IOException {
        this.path = path;
        this.credentials = ApplicationCredentials.read(path);
        this.lastModified = modifiedTime();
    }

    /**
     * The token to present now, re-read from the file if euclid has replaced it.
     *
     * @return the current bearer token
     */
    @Override
    public String get() {
        return current().token();
    }

    /**
     * The credentials as they now stand, including the identity they name.
     *
     * @return the current credentials
     */
    public ApplicationCredentials current() {
        if (hasChangedOnDisk()) {
            reload();
        }
        return credentials;
    }

    /**
     * The file being watched.
     *
     * @return the path this reads
     */
    public Path path() {
        return path;
    }

    private boolean hasChangedOnDisk() {
        final FileTime modified = modifiedTime();
        return modified != null && !modified.equals(lastModified);
    }

    private synchronized void reload() {
        final FileTime modified = modifiedTime();
        if (modified == null || modified.equals(lastModified)) {
            // Another thread got there first while this one was waiting for the lock.
            return;
        }
        try {
            credentials = ApplicationCredentials.read(path);
            lastModified = modified;
            logger.debug("Re-read euclid credentials from " + path + ", now valid until " + credentials.expiresAt());
        } catch (IOException e) {
            // Half-written files are not the explanation - euclid renames the finished file into
            // place - so this is a file that has genuinely become unreadable. Keeping the previous
            // token buys the time in which somebody can notice this line, and the next attempt is
            // the next request rather than the next rewrite.
            if (!modified.equals(warnedFor)) {
                warnedFor = modified;
                logger.warn("Could not re-read the euclid credentials file " + path
                                    + ", continuing with the token read at " + lastModified + ": " + e.getMessage());
            }
        }
    }

    private FileTime modifiedTime() {
        try {
            return Files.getLastModifiedTime(path);
        } catch (IOException e) {
            return null;
        }
    }
}
