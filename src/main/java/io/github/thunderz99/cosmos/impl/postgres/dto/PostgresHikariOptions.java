package io.github.thunderz99.cosmos.impl.postgres.dto;

import com.zaxxer.hikari.HikariConfig;
import io.github.thunderz99.cosmos.util.Checker;

import java.util.Properties;

/**
 * Optional HikariCP settings for postgres connections.
 */
public class PostgresHikariOptions {

    /**
     * Max number of connections in the pool.
     */
    public Integer maximumPoolSize;

    /**
     * Minimum number of idle connections to keep in the pool.
     */
    public Integer minimumIdle;

    /**
     * Maximum milliseconds to wait for a connection from the pool.
     */
    public Long connectionTimeoutMs;

    /**
     * Maximum milliseconds a connection can stay idle in the pool.
     */
    public Long idleTimeoutMs;

    /**
     * Maximum lifetime of a pooled connection in milliseconds.
     */
    public Long maxLifetimeMs;

    /**
     * Maximum milliseconds to wait for connection validation.
     */
    public Long validationTimeoutMs;

    /**
     * Optional pool name for diagnostics and metrics.
     */
    public String poolName;

    /**
     * Extra raw HikariCP properties for minor or future settings.
     */
    public Properties hikariProperties = new Properties();

    /**
     * Set maximum pool size.
     *
     * @param maximumPoolSize max number of connections in the pool
     * @return current option instance
     */
    public PostgresHikariOptions withMaximumPoolSize(int maximumPoolSize) {
        Checker.check(maximumPoolSize > 0, "maximumPoolSize should be > 0");
        this.maximumPoolSize = maximumPoolSize;
        return this;
    }

    /**
     * Set minimum number of idle connections in the pool.
     *
     * @param minimumIdle minimum idle connections
     * @return current option instance
     */
    public PostgresHikariOptions withMinimumIdle(int minimumIdle) {
        Checker.check(minimumIdle >= 0, "minimumIdle should be >= 0");
        this.minimumIdle = minimumIdle;
        return this;
    }

    /**
     * Set connection timeout in milliseconds.
     *
     * @param connectionTimeoutMs connection timeout in milliseconds
     * @return current option instance
     */
    public PostgresHikariOptions withConnectionTimeoutMs(long connectionTimeoutMs) {
        Checker.check(connectionTimeoutMs > 0, "connectionTimeoutMs should be > 0");
        this.connectionTimeoutMs = connectionTimeoutMs;
        return this;
    }

    /**
     * Set idle timeout in milliseconds.
     *
     * @param idleTimeoutMs idle timeout in milliseconds
     * @return current option instance
     */
    public PostgresHikariOptions withIdleTimeoutMs(long idleTimeoutMs) {
        Checker.check(idleTimeoutMs >= 0, "idleTimeoutMs should be >= 0");
        this.idleTimeoutMs = idleTimeoutMs;
        return this;
    }

    /**
     * Set max lifetime in milliseconds.
     *
     * @param maxLifetimeMs max connection lifetime in milliseconds
     * @return current option instance
     */
    public PostgresHikariOptions withMaxLifetimeMs(long maxLifetimeMs) {
        Checker.check(maxLifetimeMs >= 0, "maxLifetimeMs should be >= 0");
        this.maxLifetimeMs = maxLifetimeMs;
        return this;
    }

    /**
     * Set validation timeout in milliseconds.
     *
     * @param validationTimeoutMs validation timeout in milliseconds
     * @return current option instance
     */
    public PostgresHikariOptions withValidationTimeoutMs(long validationTimeoutMs) {
        Checker.check(validationTimeoutMs > 0, "validationTimeoutMs should be > 0");
        this.validationTimeoutMs = validationTimeoutMs;
        return this;
    }

    /**
     * Set pool name.
     *
     * @param poolName pool name for diagnostics
     * @return current option instance
     */
    public PostgresHikariOptions withPoolName(String poolName) {
        Checker.checkNotBlank(poolName, "poolName");
        this.poolName = poolName;
        return this;
    }

    /**
     * Add one raw HikariCP property.
     *
     * @param key hikari property key
     * @param value hikari property value
     * @return current option instance
     */
    public PostgresHikariOptions withHikariProperty(String key, Object value) {
        Checker.checkNotBlank(key, "key");
        Checker.checkNotNull(value, "value");
        this.hikariProperties.put(key, value);
        return this;
    }

    /**
     * Merge raw HikariCP properties.
     *
     * @param properties hikari properties
     * @return current option instance
     */
    public PostgresHikariOptions withHikariProperties(Properties properties) {
        Checker.checkNotNull(properties, "properties");
        this.hikariProperties.putAll(properties);
        return this;
    }

    /**
     * Return a copy of custom raw HikariCP properties.
     *
     * @return copied properties
     */
    public Properties getHikariProperties() {
        var copied = new Properties();
        copied.putAll(this.hikariProperties);
        return copied;
    }

    /**
     * Apply options to a HikariConfig.
     *
     * @param config Hikari config to mutate
     */
    public void applyTo(HikariConfig config) {
        Checker.checkNotNull(config, "config");

        if (maximumPoolSize != null) {
            config.setMaximumPoolSize(maximumPoolSize);
        }

        if (minimumIdle != null) {
            config.setMinimumIdle(minimumIdle);
        }

        if (connectionTimeoutMs != null) {
            config.setConnectionTimeout(connectionTimeoutMs);
        }

        if (idleTimeoutMs != null) {
            config.setIdleTimeout(idleTimeoutMs);
        }

        if (maxLifetimeMs != null) {
            config.setMaxLifetime(maxLifetimeMs);
        }

        if (validationTimeoutMs != null) {
            config.setValidationTimeout(validationTimeoutMs);
        }

        if (poolName != null) {
            config.setPoolName(poolName);
        }
    }
}
