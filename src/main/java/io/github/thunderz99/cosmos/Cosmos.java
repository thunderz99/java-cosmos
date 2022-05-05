package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.microsoft.azure.documentdb.*;
import io.github.thunderz99.cosmos.util.Checker;
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
    
    String account;

    static Pattern connectionStringPattern = Pattern.compile("AccountEndpoint=(?<endpoint>.+);AccountKey=(?<key>[^;]+);?");

    public static final String JC_SDK_V4_ENABLE = "JC_SDK_V4_ENABLE";

    public Cosmos(String connectionString) {
        this(connectionString, null);
    }

    public Cosmos(String connectionString, List<String> preferredRegions) {

        Pair<String, String> pair = parseConnectionString(connectionString);
        var endpoint = pair.getLeft();
        var key = pair.getRight();

        this.client = new DocumentClient(endpoint, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

        //var v4Enable = Boolean.parseBoolean(EnvUtil.getOrDefault(Cosmos.JC_SDK_V4_ENABLE, "false"));

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
     * Create the db and coll if not exist. Coll creation will be skipped if empty. uniqueKeyPolicy can be specified.
     *
     * @param db              database name
     * @param coll            collection name
     * @param uniqueKeyPolicy unique key policy for the collection
     * @return CosmosDatabase instance
     * @throws DocumentClientException Cosmos client exception Cosmos client exception
     */
    public CosmosDatabase createIfNotExist(String db, String coll, UniqueKeyPolicy uniqueKeyPolicy) throws DocumentClientException {

        if (StringUtils.isBlank(coll)) {
            createDatabaseIfNotExist(client, db);
        } else {
            createCollectionIfNotExist(client, db, coll, uniqueKeyPolicy);
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

        if (StringUtils.isBlank(coll)) {
            createDatabaseIfNotExist(client, db);
        } else {
            createCollectionIfNotExist(client, db, coll);
        }

        return new CosmosDatabase(this, db);
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

    static Database createDatabaseIfNotExist(DocumentClient client, String db) throws DocumentClientException {

        var database = readDatabase(client, db);
        if (database != null) {
            return database;
        }

        var account = getAccount(client);

        var dbObj = new Database();
        dbObj.setId(db);
        var options = new RequestOptions();
        options.setOfferThroughput(400);
        var result = client.createDatabase(dbObj, options);
        log.info("created database:{}, account:{}", db, account);
        return result.getResource();
    }

    static Database readDatabase(DocumentClient client, String db) throws DocumentClientException {
        try {
            Checker.checkNotBlank(db, "db");
            var res =
                    client.readDatabase(getDatabaseLink(db), null);
            return res.getResource();
        } catch (DocumentClientException de) {
            // If not exist
            if (isResourceNotFoundException(de)) {
                return null;
            } else {
                // Throw any other Exception
                throw de;
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

    static DocumentCollection createCollectionIfNotExist(DocumentClient client, String db, String coll) throws DocumentClientException {
        return createCollectionIfNotExist(client, db, coll, getDefaultUniqueKeyPolicy());
    }

    static DocumentCollection createCollectionIfNotExist(DocumentClient client, String db, String coll, UniqueKeyPolicy uniqueKeyPolicy) throws DocumentClientException {

        createDatabaseIfNotExist(client, db);

        var collection = readCollection(client, db, coll);

        if (collection != null) {
            return collection;
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
     * @throws DocumentClientException
     */
    public DocumentCollection readCollection(String db, String coll) throws DocumentClientException {
        return readCollection(client, db, coll);
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


    static String getDatabaseLink(String db) {
        return String.format("/dbs/%s", db);
    }

    static String getCollectionLink(String db, String coll) {
        return String.format("/dbs/%s/colls/%s", db, coll);
    }

    static String getDocumentLink(String db, String coll, String id) {
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

    public String getAccount() throws DocumentClientException {
        if (StringUtils.isNotEmpty(this.account)) {
            return this.account;
        }
        this.account = getAccount(this.client);
        return this.account;
    }

    /**
     * Get cosmos db account id from client
     *
     * @param client documentClient
     * @return cosmos db account id
     * @throws DocumentClientException Cosmos client exception
     */
    static String getAccount(DocumentClient client) throws DocumentClientException {
        if (Objects.isNull(client)) {
            return "";
        }
        return client.getDatabaseAccount().get("id").toString();
    }

    static Pair<String, String> parseConnectionString(String connectionString) {

        var matcher = connectionStringPattern.matcher(connectionString);
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "Make sure connectionString contains 'AccountEndpoint=' and 'AccountKey=' ");
        }
        String endpoint = matcher.group("endpoint");
        String key = matcher.group("key");

        Checker.check(StringUtils.isNotBlank(endpoint), "Make sure connectionString contains 'AccountEndpoint=' ");
        Checker.check(StringUtils.isNotBlank(key), "Make sure connectionString contains 'AccountKey='");

        if (log.isInfoEnabled()) {
            log.info("endpoint:{}", endpoint);
            log.info("key:{}...", key.substring(0, 3));
        }

        return Pair.of(endpoint, key);
    }
}