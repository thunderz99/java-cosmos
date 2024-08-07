package io.github.thunderz99.cosmos;

import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosImpl;
import io.github.thunderz99.cosmos.util.Checker;

/**
 * Builder class to build a cosmos instance (for azure cosmosdb or mongodb)
 */
public class CosmosBuilder {

    String dbType = "cosmosdb";

    String connectionString;

    /**
     * Specify the dbType( "cosmosdb" or "mongodb" )
     * @param dbType
     * @return cosmosBuilder
     */
    public CosmosBuilder withDatabaseType(String dbType){
        this.dbType = dbType;
        return this;
    }

    public CosmosBuilder withConnectionString(String connectionString){
        this.connectionString = connectionString;
        return this;
    }

    /**
     * Build the instance representing a Cosmos instance.
     * @return Cosmos instance
     */
    public Cosmos build(){
        Checker.checkNotBlank(dbType, "dbType");
        Checker.checkNotBlank(connectionString, "connectionString");
        return new CosmosImpl(connectionString);
    }


}

