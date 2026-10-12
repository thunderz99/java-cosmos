package io.github.thunderz99.cosmos.impl;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared resource closing and JVM hook registration for database clients.
 * Business task draining and shutdown ordering belong to the caller.
 */
public final class ClientLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ClientLifecycle.class);

    private final Runnable closeAction;
    private Thread shutdownHook;
    private boolean closed;

    public ClientLifecycle(Runnable closeAction) {
        this.closeAction = Objects.requireNonNull(closeAction, "closeAction");
    }

    /** Register once, after the owning client has completed initialization. */
    public synchronized void registerShutdownHook() {
        if (closed) {
            throw new IllegalStateException("Cannot register a shutdown hook for a closed client");
        }
        if (shutdownHook != null) {
            return;
        }
        var hook = new Thread(() -> {
            try {
                close();
            } catch (RuntimeException | Error failure) {
                // close() has logged the failure; no caller is available during JVM shutdown.
            }
        }, "java-cosmos-client-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        shutdownHook = hook;
    }

    /**
     * Serialize close attempts, marking completion only after the action succeeds.
     * Failed attempts remain retryable and keep their registered hook.
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            closeAction.run();
        } catch (RuntimeException | Error failure) {
            LOG.warn("Failed to close database client; a subsequent close may retry", failure);
            throw failure;
        }
        closed = true;
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException shutdownInProgress) {
                // The JVM forbids removing hooks once shutdown has started.
                LOG.debug("JVM shutdown already started; client shutdown hook cannot be removed");
            } catch (SecurityException denied) {
                LOG.warn("Database client closed but shutdown hook removal was denied", denied);
            }
            shutdownHook = null;
        }
    }

    /** Preserve the original initialization failure even if resource cleanup also fails. */
    public void closeOnInitializationFailure(Throwable failure) {
        try {
            close();
        } catch (RuntimeException | Error cleanupFailure) {
            if (cleanupFailure != failure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }
}
