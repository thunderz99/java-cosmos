package io.github.thunderz99.cosmos;

import java.util.List;

import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosImpl;
import io.github.thunderz99.cosmos.impl.mongo.MongoImpl;
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


    String dbType = COSMOSDB;

    String connectionString;

    List<String> preferredRegions;

    boolean expireAtEnabled = false;

    /**
     * Specify the dbType( "cosmosdb" or "mongodb" )
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
     * Specify the preferredRegions for cosmosdb. Note there is no effect to mongodb.
     *
     * @param preferredRegions
     * @return
     */
    public CosmosBuilder withPreferredRegions(List<String> preferredRegions) {
        this.preferredRegions = preferredRegions;
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
     * @return
     */
    public CosmosBuilder withExpireAtEnabled(boolean enabled) {
        this.expireAtEnabled = enabled;
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
            return new MongoImpl(connectionString, expireAtEnabled);
        }

        throw new IllegalArgumentException("Not supported dbType: " + dbType);

    }


}

