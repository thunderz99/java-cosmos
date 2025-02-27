package io.github.thunderz99.cosmos;

import java.util.List;

import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosImpl;
import io.github.thunderz99.cosmos.impl.mongo.MongoImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.util.PGSortUtil;
import io.github.thunderz99.cosmos.util.Checker;
import org.apache.commons.lang3.StringUtils;

/**
 * Builder class to build a cosmos instance (for azure cosmosdb or mongodb)
 */
public class CosmosBuilder {

    /**
     * Constant for dbType: cosmosdb
     */
    public static String COSMOSDB = "cosmosdb";

    /**
     * Constant for dbType: mongodb
     */
    public static String MONGODB = "mongodb";

    /**
     * Constant for dbType: postgres
     */
    public static String POSTGRES = "postgres";


    String dbType = COSMOSDB;

    String connectionString;

    List<String> preferredRegions;

    /**
     * db sort settings. only affects postgres. see docs/postgres-sort-order.md for details
     *
     * <p>
     *     default to "en_US"
     * </p>
     */
    String collate = PGSortUtil.COLLATE_EN_US;

    boolean expireAtEnabled = false;

    boolean etagEnabled = false;

    /**
     * Specify the dbType( "cosmosdb" or "mongodb" or "postgres")
     *
     * @param dbType
     * @return cosmosBuilder
     */
    public CosmosBuilder withDatabaseType(String dbType) {
        this.dbType = dbType;
        return this;
    }

    /**
     * Specify the connectionString for cosmosdb or mongodb
     *
     * <pre>
     *     e.g.:
     *     COSMOSDB_CONNECTION_STRING=AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=yyy==;
     *     MONGODB_CONNECTION_STRING=mongodb://localhost:27017;
     * </pre>
     *
     * @param connectionString
     * @return
     */
    public CosmosBuilder withConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    /**
     * Specify the preferredRegions for cosmosdb. Note there is no effect to mongodb/postgres.
     *
     * @param preferredRegions
     * @return
     */
    public CosmosBuilder withPreferredRegions(List<String> preferredRegions) {
        this.preferredRegions = preferredRegions;
        return this;
    }

    /**
     * Specify the collate setting for postgres(affects sort order). Note there is no effect to cosmosdb/mongodb.
     *
     * <p>
     *     see docs/postgres-sort-order.md for details
     * </p>
     *
     * @param collate "C" or "en_US", default to "en_US"
     * @return
     */
    public CosmosBuilder withCollate(String collate) {
        this.collate = collate;
        return this;
    }

    /**
     * Specify whether enable the expireAt feature for mongodb. Note there is no effect to cosmosdb.
     *
     * <p>
     * This option is to mimic the cosmosdb's ttl feature.
     * This is implemented by adding an "_expireAt" field automatically when "ttl" field has value.
     * The value of "expireAt" field would be set to the lastModified timestamp + "ttl" in seconds.
     * And a TTL index is created for "expireAt" field(Note index should be added by yourself).
     * So the document will automatically be deleted at the specific timestamp.
     * </p>
     *
     * @param enabled
     * @return this
     */
    public CosmosBuilder withExpireAtEnabled(boolean enabled) {
        this.expireAtEnabled = enabled;
        return this;
    }

    /**
     * Specify whether enable the etag feature for mongodb. Note there is no effect to cosmosdb.
     *
     * <p>
     * if enabled, "_etag" field is automatically added when modify a document, for optimistic lock.
     * default is disabled.
     * </p>
     *
     * @param enabled
     * @return this
     */
    public CosmosBuilder withEtagEnabled(boolean enabled) {
        this.etagEnabled = enabled;
        return this;
    }

    /**
     * Build the instance representing a Cosmos instance.
     *
     * @return Cosmos instance
     */
    public Cosmos build() {
        Checker.checkNotBlank(dbType, "dbType");
        Checker.checkNotBlank(connectionString, "connectionString");

        if (StringUtils.equals(dbType, COSMOSDB)) {
            return new CosmosImpl(connectionString, preferredRegions);
        }

        if (StringUtils.equals(dbType, MONGODB)) {
            return new MongoImpl(connectionString, expireAtEnabled, etagEnabled);
        }

        if (StringUtils.equals(dbType, POSTGRES)) {
            return new PostgresImpl(connectionString, expireAtEnabled, etagEnabled, collate);
        }

        throw new IllegalArgumentException("Not supported dbType: " + dbType);

    }


}

