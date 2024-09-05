package io.github.thunderz99.cosmos.impl.cosmosdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosContainerProperties;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosBuilder;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosContainerResponse;
import io.github.thunderz99.cosmos.dto.UniqueKeyPolicy;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.ConnectionStringUtil;
import io.github.thunderz99.cosmos.util.LinkFormatUtil;
import io.github.thunderz99.cosmos.util.UniqueKeyUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * class that represent a cosmos account
 *
 * <pre>
 * Usage: var cosmos = new
 * Cosmos("AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=xxx==;");
 * var db = cosmos.getDatabase("Database1");
 *
 * //Then use db to do CRUD / query db.upsert("Users", user);
 * </pre>
 *
 */
public class CosmosImpl implements Cosmos {

    private static Logger log = LoggerFactory.getLogger(CosmosImpl.class);

    CosmosClient client;

    String account;

    public static final String COSMOS_SDK_V4_ENABLE = "COSMOS_SDK_V4_ENABLE";

    public static final String ETAG = "_etag";

    public CosmosImpl(String connectionString) {
        this(connectionString, null);
    }

    public CosmosImpl(String connectionString, List<String> preferredRegions) {

        Pair<String, String> pair = ConnectionStringUtil.parseConnectionString(connectionString);
        var endpoint = pair.getLeft();
        var key = pair.getRight();

        log.info("COSMOS_SDK_V4_ENABLE is enabled for endpoint:{}", endpoint);
        this.client = new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .preferredRegions(preferredRegions)
                .consistencyLevel(com.azure.cosmos.ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        this.account = extractAccountName(endpoint);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.closeClient();
        }));

    }


    /**
     * Get a CosmosDatabase object by name
     *
     * @param db database name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
        return new CosmosDatabaseImpl(this, db);
    }


    /**
     * extract the Cosmos DB 's account name from the endpoint
     *
     * @param endpoint the endpoint for Cosmos DB
     * @return account name
     */
    static String extractAccountName(String endpoint) {
        try {
            var uri = new URI(endpoint);
            String host = uri.getHost();
            return StringUtils.isEmpty(host) ? "" : host.split("\\.")[0];
        } catch (URISyntaxException e) {
            log.error("Error parsing endpoint URI", e);
            return "";
        }
    }

    /**
     * Create the db and coll if not exist. Coll creation will be skipped if empty. uniqueKeyPolicy can be specified.
     *
     * @param db              database name
     * @param coll            collection name
     * @param uniqueKeyPolicy unique key policy for the collection
     * @return CosmosDatabase instance
     * @throws CosmosException Cosmos client exception
     */
    public CosmosDatabase createIfNotExist(String db, String coll, UniqueKeyPolicy uniqueKeyPolicy) throws CosmosException {

        Checker.checkNotBlank(db, "Database name");

        this.client.createDatabaseIfNotExists(db);
        log.info("created database:{}, account:{}", db, account);

        if (StringUtils.isBlank(coll)) {
            //do nothing
        } else {
            var cosmosDatabase = this.client.getDatabase(db);
            var containerProperties = new CosmosContainerProperties(coll, "/" + getDefaultPartitionKey());

            var keyList = uniqueKeyPolicy.uniqueKeys;
            if (CollectionUtils.isNotEmpty(keyList)) {
                containerProperties.setUniqueKeyPolicy(UniqueKeyUtil.toCosmosUniqueKeyPolicy(uniqueKeyPolicy));
            }
            cosmosDatabase.createContainerIfNotExists(containerProperties);
        }

        return new CosmosDatabaseImpl(this, db);
    }

    /**
     * Create the db and coll if not exist. Coll creation will be skipped if empty.
     *
     * <p>
     * No uniqueKeyPolicy will be used.
     * </p>
     *
     * @param db   database name
     * @param coll collection name
     * @return CosmosDatabase instance
     * @throws CosmosException Cosmos client exception Cosmos client exception
     */
    public CosmosDatabase createIfNotExist(String db, String coll) throws CosmosException {
        return createIfNotExist(db, coll, getDefaultUniqueKeyPolicy());
    }

    /**
     * Delete a database by name
     *
     * @param db database name
     * @throws CosmosException Cosmos client exception
     */
    public void deleteDatabase(String db) throws CosmosException {
        if (StringUtils.isEmpty(db)) {
            return;
        }
        var cosmosDatabase = this.client.getDatabase(db);
        try {
            cosmosDatabase.delete();
        } catch (com.azure.cosmos.CosmosException ce) {
            if (isResourceNotFoundException(ce)) {
                log.info("delete Database not exist. Ignored:{}, account:{}", LinkFormatUtil.getDatabaseLink(db), this.account);
            } else {
                throw ce;
            }
        }
    }

    /**
     * Delete a collection by db name and coll name
     *
     * @param db   database name
     * @param coll collection name
     * @throws CosmosException Cosmos client exception
     */
    public void deleteCollection(String db, String coll) throws CosmosException {

        var cosmosDatabase = this.client.getDatabase(db);
        var container = cosmosDatabase.getContainer(coll);
        try {
            container.delete();
        } catch (com.azure.cosmos.CosmosException ce) {
            // If not exist
            if (isResourceNotFoundException(ce)) {
                log.info("delete Collection not exist. Ignored:{}, account:{}", LinkFormatUtil.getCollectionLink(db, coll), this.account);
            } else {
                // Throw any other Exception
                throw ce;
            }
        }
    }

    /**
     * Read the document collection obj by dbName and collName.
     *
     * @param db   dbName
     * @param coll collName
     * @return CosmosContainerResponse obj
     * @throws CosmosException when client exception occurs
     */
    public CosmosContainerResponse readCollection(String db, String coll) throws CosmosException {


        var database = this.client.getDatabase(db);
        try {
            var response = database.getContainer(coll).read();
            var policy = UniqueKeyUtil.toCommonUniqueKeyPolicy(response.getProperties().getUniqueKeyPolicy());
            return new CosmosContainerResponse(coll, policy);

        } catch (com.azure.cosmos.CosmosException ce) {
            if (isResourceNotFoundException(ce)) {
                return null;
            }
            throw ce;
        }
    }


    /**
     * Get Official cosmos db sdk(v4)'s CosmosClient instance (the same to getClient(), for compatibility)
     *
     * @return official cosmosdb sdk client(v4)
     */
    public CosmosClient getClientV4() {
        return this.client;
    }

    /**
     * Get Official cosmos db sdk(v4)'s CosmosClient instance
     *
     * @return official cosmosdb sdk client(v4)
     */
    public CosmosClient getClient() {
        return this.client;
    }

    /**
     * return the default unique key policy
     *
     * @return default unique key policy
     */
    public static UniqueKeyPolicy getDefaultUniqueKeyPolicy() {
        var uniqueKeyPolicy = new UniqueKeyPolicy();
        return uniqueKeyPolicy;
    }

    public static boolean isResourceNotFoundException(Exception e) {
        if (e instanceof CosmosException) {
            var ce = (CosmosException) e;
            return ce.getStatusCode() == 404;
        }
        return StringUtils.contains(e.getMessage(), "Resource Not Found") ? true : false;
    }

    /**
     * get the default partition key
     *
     * @return default partition key
     */
    public static String getDefaultPartitionKey() {
        return "_partition";
    }

    public String getAccount() throws CosmosException {
        return account;
    }

    @Override
    public String getDatabaseType() {
        return CosmosBuilder.COSMOSDB;
    }

    /**
     * Close the internal database client safely
     */
    @Override
    public void closeClient() {
        this.getClient().close();
    }

}