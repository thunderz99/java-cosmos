package io.github.thunderz99.cosmos;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.UniqueKey;
import com.azure.cosmos.models.UniqueKeyPolicy;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.ConnectionStringUtil;
import io.github.thunderz99.cosmos.util.EnvUtil;
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
public class Cosmos {

    private static Logger log = LoggerFactory.getLogger(Cosmos.class);

    CosmosClient clientV4;

    String account;

    public static final String COSMOS_SDK_V4_ENABLE = "COSMOS_SDK_V4_ENABLE";

    public static final String ETAG = "_etag";

    public Cosmos(String connectionString) {
        this(connectionString, null);
    }

    public Cosmos(String connectionString, List<String> preferredRegions) {

        Pair<String, String> pair = ConnectionStringUtil.parseConnectionString(connectionString);
        var endpoint = pair.getLeft();
        var key = pair.getRight();

        // default to true
        var v4Enable = Boolean.parseBoolean(EnvUtil.getOrDefault(Cosmos.COSMOS_SDK_V4_ENABLE, "true"));

        if (v4Enable) {
            log.info("COSMOS_SDK_V4_ENABLE is enabled for endpoint:{}", endpoint);
            this.clientV4 = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .preferredRegions(preferredRegions)
                    .consistencyLevel(com.azure.cosmos.ConsistencyLevel.SESSION)
                    .contentResponseOnWriteEnabled(true)
                    .buildClient();

            this.account = extractAccountName(endpoint);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                this.clientV4.close();
            }));
        }


    }


    /**
     * Get a CosmosDatabase object by name
     *
     * @param db database name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
        return new CosmosDatabase(this, db);
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

        this.clientV4.createDatabaseIfNotExists(db);
        log.info("created database:{}, account:{}", db, account);

        if (StringUtils.isBlank(coll)) {
            //do nothing
        } else {
            var cosmosDatabase = this.clientV4.getDatabase(db);
            var containerProperties = new CosmosContainerProperties(coll, "/" + getDefaultPartitionKey());

            var keyList = uniqueKeyPolicy.getUniqueKeys();
            if (!keyList.isEmpty()) {
                containerProperties.setUniqueKeyPolicy(uniqueKeyPolicy);
            }
            cosmosDatabase.createContainerIfNotExists(containerProperties);
        }

        return new CosmosDatabase(this, db);
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
        var cosmosDatabase = this.clientV4.getDatabase(db);
        try {
            cosmosDatabase.delete();
        } catch (com.azure.cosmos.CosmosException ce) {
            if (isResourceNotFoundException(ce)) {
                log.info("delete Database not exist. Ignored:{}, account:{}", getDatabaseLink(db), this.account);
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

        var cosmosDatabase = this.clientV4.getDatabase(db);
        var container = cosmosDatabase.getContainer(coll);
        try {
            container.delete();
        } catch (com.azure.cosmos.CosmosException ce) {
            // If not exist
            if (isResourceNotFoundException(ce)) {
                log.info("delete Collection not exist. Ignored:{}, account:{}", getCollectionLink(db, coll), this.account);
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


        var database = this.clientV4.getDatabase(db);
        try {
            return database.getContainer(coll).read();
        } catch (com.azure.cosmos.CosmosException ce) {
            if (isResourceNotFoundException(ce)) {
                return null;
            }
            throw ce;
        }
    }


    /**
     * Get Official cosmos db sdk(v4)'s CosmosClient instance
     *
     * @return official cosmosdb sdk client(v4)
     */
    public CosmosClient getClientV4() {
        return this.clientV4;
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

    /**
     * return the unique key policy by key
     *
     * <p>
     * key starts with "/".  e.g.  "/users/title"
     * </p>
     *
     * @param keys fields to generate uniqueKeyPolicy
     * @return unique key policy
     */
    public static UniqueKeyPolicy getUniqueKeyPolicy(Set<String> keys) {
        var uniqueKeyPolicy = new UniqueKeyPolicy();

        if (CollectionUtils.isEmpty(keys)) {
            return uniqueKeyPolicy;
        }

        uniqueKeyPolicy.setUniqueKeys(keys.stream().map(key -> toUniqueKey(key)).collect(Collectors.toList()));

        return uniqueKeyPolicy;
    }

    /**
     * generate a uniqueKey obj from a string
     *
     * @param key
     * @return
     */
    static UniqueKey toUniqueKey(String key) {
        Checker.checkNotBlank(key, "uniqueKey");
        var ret = new UniqueKey(List.of(key));
        return ret;
    }


    /**
     * Generate database link format used in cosmosdb
     *
     * @param db db name
     * @return databaseLink
     */
    public static String getDatabaseLink(String db) {
        return String.format("/dbs/%s", db);
    }

    /**
     * Generate database link format used in cosmosdb
     *
     * @param db   db name
     * @param coll collection name
     * @return collection link
     */
    public static String getCollectionLink(String db, String coll) {
        return String.format("/dbs/%s/colls/%s", db, coll);
    }

    /**
     * Generate document link format used in cosmosdb
     *
     * @param db   db name
     * @param coll collection name
     * @param id   document id
     * @return document link
     */
    public static String getDocumentLink(String db, String coll, String id) {
        return String.format("/dbs/%s/colls/%s/docs/%s", db, coll, id);
    }

    static boolean isResourceNotFoundException(Exception e) {
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

}