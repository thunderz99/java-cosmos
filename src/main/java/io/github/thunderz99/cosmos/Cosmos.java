package io.github.thunderz99.cosmos;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.microsoft.azure.documentdb.*;
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

    DocumentClient client;

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

        this.client = new DocumentClient(endpoint, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

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
    String extractAccountName(String endpoint) {
        try {
            var uri = new URI(endpoint);
            String host = uri.getHost();
            return host.split("\\.")[0];
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
     * @throws DocumentClientException Cosmos client exception Cosmos client exception
     */
    public CosmosDatabase createIfNotExist(String db, String coll, UniqueKeyPolicy uniqueKeyPolicy) throws CosmosException {

        if (StringUtils.isBlank(coll)) {
            this.clientV4.createDatabaseIfNotExists(db);
            log.info("created database:{}, account:{}", db, account);
        } else {
            this.createCollectionIfNotExist(client, db, coll, uniqueKeyPolicy);
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
     * @throws DocumentClientException Cosmos client exception Cosmos client exception
     */
    public CosmosDatabase createIfNotExist(String db, String coll) throws DocumentClientException {
        return createIfNotExist(db, coll, getDefaultUniqueKeyPolicy());
    }

    /**
     * Delete a database by name
     *
     * @param db database name
     * @throws DocumentClientException Cosmos client exception
     */
    public void deleteDatabase(String db) throws DocumentClientException {
        deleteDatabase(client, db);
    }

    /**
     * Delete a collection by db name and coll name
     *
     * @param db   database name
     * @param coll collection name
     * @throws DocumentClientException Cosmos client exception
     */
    public void deleteCollection(String db, String coll) throws DocumentClientException {
        deleteCollection(client, db, coll);
    }

    static com.azure.cosmos.CosmosDatabase readDatabase(CosmosClient client, String db) throws DocumentClientException {
        try {
            Checker.checkNotBlank(db, "db");
            var res =
                    client.getDatabase(db);
            return res;
        } catch (com.azure.cosmos.CosmosException ce) {
            // If not exist
            if (isResourceNotFoundException(ce)) {
                return null;
            } else {
                // Throw any other Exception
                throw ce;
            }
        }
    }

    static void deleteDatabase(DocumentClient client, String db) throws DocumentClientException {
        try {
            Checker.checkNotNull(client, "client");
            Checker.checkNotBlank(db, "db");
            client.deleteDatabase(getDatabaseLink(db), null);
            log.info("delete Database:{}, account:{}", db, getAccount(client));
        } catch (DocumentClientException de) {
            // If not exist
            if (isResourceNotFoundException(de)) {
                log.info("delete Database not exist. Ignored:{}, account:{}", db, getAccount(client));
            } else {
                // Throw any other Exception
                throw de;
            }
        }
    }

    static void deleteCollection(DocumentClient client, String db, String coll) throws DocumentClientException {
        var collectionLink = getCollectionLink(db, coll);
        try {
            Checker.checkNotNull(client, "client");
            Checker.checkNotBlank(db, "db");
            Checker.checkNotBlank(coll, "coll");
            client.deleteCollection(collectionLink, null);
            log.info("delete Collection:{}, account:{}", collectionLink, getAccount(client));
        } catch (DocumentClientException de) {
            // If not exist
            if (isResourceNotFoundException(de)) {
                log.info("delete Collection not exist. Ignored:{}, account:{}", collectionLink, getAccount(client));
            } else {
                // Throw any other Exception
                throw de;
            }
        }
    }

    CosmosContainer createCollectionIfNotExist(String db, String coll) throws DocumentClientException {
        return createCollectionIfNotExist(db, coll, getDefaultUniqueKeyPolicy());
    }

    CosmosContainer createCollectionIfNotExist(String db, String coll, UniqueKeyPolicy uniqueKeyPolicy) throws DocumentClientException {

        this.clientV4.createDatabaseIfNotExists(db);
        log.info("created database:{}, account:{}", db, account);

        var container = this.clientV4.getDatabase(db).createContainerIfNotExists()

        if (container != null) {
            return container;
        }

        var collectionInfo = new DocumentCollection();
        collectionInfo.setId(coll);

        collectionInfo.setIndexingPolicy(getDefaultIndexingPolicy());

        if (uniqueKeyPolicy != null) {
            collectionInfo.setUniqueKeyPolicy(uniqueKeyPolicy);
        }

        collectionInfo.setDefaultTimeToLive(-1);

        var partitionKeyDef = new PartitionKeyDefinition();
        var paths = List.of("/" + getDefaultPartitionKey());
        partitionKeyDef.setPaths(paths);

        collectionInfo.setPartitionKey(partitionKeyDef);

        var databaseLink = getDatabaseLink(db);
        var createdColl = client.createCollection(databaseLink, collectionInfo, null);
        log.info("create Collection:{}/colls/{}, account:{}", databaseLink, coll, getAccount(client));

        return createdColl.getResource();

    }

    /**
     * Read the document collection obj by dbName and collName.
     *
     * @param db   dbName
     * @param coll collName
     * @return documentCollection obj
     * @throws DocumentClientException when client exception occurs
     */
    public DocumentCollection readCollection(String db, String coll) throws DocumentClientException {
        return readCollection(client, db, coll);
    }


    /**
     * Get Official cosmos db sdk(v2)'s DocumentClient instance
     *
     * @return official cosmosdb sdk client(v2)
     */
    public DocumentClient getClient() {
        return this.client;
    }

    /**
     * Get Official cosmos db sdk(v4)'s CosmosClient instance
     *
     * @return official cosmosdb sdk client(v4)
     */
    public CosmosClient getClientV4() {
        return this.clientV4;
    }
    

    static DocumentCollection readCollection(DocumentClient client, String db, String coll)
            throws DocumentClientException {
        try {
            var res = client.readCollection(getCollectionLink(db, coll), null);
            return res.getResource();
        } catch (DocumentClientException de) {
            // if Not Found
            if (de.getStatusCode() == 404) {
                return null;
            } else {
                // Throw other exceptions
                throw de;
            }
        }
    }

    /**
     * return the default indexingPolicy
     *
     * @return default indexingPolicy
     */
    static IndexingPolicy getDefaultIndexingPolicy() {
        var rangeIndexOverride = new Index[3];
        rangeIndexOverride[0] = Index.Range(DataType.Number, -1);
        rangeIndexOverride[1] = Index.Range(DataType.String, -1);
        rangeIndexOverride[2] = Index.Spatial(DataType.Point);
        var indexingPolicy = new IndexingPolicy(rangeIndexOverride);
        indexingPolicy.setIndexingMode(IndexingMode.Consistent);
        log.info("set indexing policy to default: {} ", indexingPolicy);
        return indexingPolicy;
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
        var ret = new UniqueKey();
        ret.setPaths(List.of(key));
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