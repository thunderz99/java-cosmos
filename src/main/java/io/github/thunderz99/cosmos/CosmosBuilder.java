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
            return new MongoImpl(connectionString);
        }

        throw new IllegalArgumentException("Not supported dbType: " + dbType);

    }


}

