package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.models.UniqueKey;
import com.azure.cosmos.models.UniqueKeyPolicy;
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

    CosmosClient client;

    String account;

    static Pattern connectionStringPattern = Pattern.compile("AccountEndpoint=(?<endpoint>.+);AccountKey=(?<key>.+);");
    static Pattern accountPattern = Pattern.compile("https://(?<account>[\\w\\-]+)\\..*");

    public Cosmos(String connectionString) {

        Pair<String, String> pair = parseConnectionString(connectionString);
        var endpoint = pair.getLeft();
        var key = pair.getRight();

        this.account = parseAcount(endpoint);

        this.client = new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .preferredRegions(List.of("Japan East", "West US"))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .contentResponseOnWriteEnabled(true)
                .buildClient();
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


    static String parseAcount(String endpoint) {
        var matcher = accountPattern.matcher(endpoint);
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "Make sure endpoint matches https://xxx.yyy ");
        }
        var account = matcher.group("account");
        Checker.check(StringUtils.isNotBlank(account), "Make sure account is correct");

        return account;
    }


    /**
     * Get a CosmosDatabase object by name
     *
     * @param db database name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
        return new CosmosDatabase(client, db, this);
    }


    /**
     * Create the db and cont if not exist. Coll creation will be skipped if empty. uniqueKeyPolicy can be specified.
     *
     * @param db              database name
     * @param cont            container name
     * @param uniqueKeyPolicy unique key policy for the container
     * @return CosmosDatabase instance
     */
    public CosmosDatabase createIfNotExist(String db, String cont, UniqueKeyPolicy uniqueKeyPolicy) {

        client.createDatabaseIfNotExists(db);

        if (StringUtils.isNotBlank(cont)) {
            createCollectionIfNotExist(db, cont, uniqueKeyPolicy);
        }

        return new CosmosDatabase(client, db, this);
    }

    /**
     * Create the db and cont if not exist. Coll creation will be skipped if empty.
     *
     * <p>
     * No uniqueKeyPolicy will be used.
     * </p>
     *
     * @param db   database name
     * @param cont container name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase createIfNotExist(String db, String cont) {
        return createIfNotExist(db, cont, getDefaultUniqueKeyPolicy());
    }

    /**
     * Delete a database by name
     *
     * @param db database name
     */
    public void deleteDatabase(String db) {
        try {
            Checker.checkNotNull(client, "client");
            Checker.checkNotBlank(db, "db");
            var database = client.getDatabase(db);
            database.delete();
            log.info("delete Database:{}, account:{}", db, account);
        } catch (CosmosException e) {
            // If not exist
            if (isResourceNotFoundException(e)) {
                log.info("delete Database not exist. Ignored:{}, account:{}", db, account);
            } else {
                // Throw any other Exception
                throw e;
            }
        }
    }


    /**
     * Delete a container by db name and cont name
     *
     * @param db   database name
     * @param cont container name
     */
    public void deleteCollection(String db, String cont) {
        var collectionLink = getCollectionLink(db, cont);
        try {
            Checker.checkNotNull(client, "client");
            Checker.checkNotBlank(db, "db");
            Checker.checkNotBlank(cont, "cont");
            var database = client.getDatabase(db);
            var container = database.getContainer(cont);
            container.delete();
            log.info("delete container:{}, account:{}", collectionLink, this.account);
        } catch (CosmosException e) {
            // If not exist
            if (isResourceNotFoundException(e)) {
                log.info("delete container not exist. Ignored:{}, account:{}", collectionLink, this.account);
            } else {
                // Throw any other Exception
                throw e;
            }
        }
    }


    public CosmosContainer createCollectionIfNotExist(String db, String cont) {
        return createCollectionIfNotExist(db, cont, getDefaultUniqueKeyPolicy());
    }

    public CosmosContainer createCollectionIfNotExist(String db, String cont, UniqueKeyPolicy uniqueKeyPolicy) {

        var database = client.getDatabase(db);

        var containerProperties =
                new CosmosContainerProperties(cont, "/" + getDefaultPartitionKey());
        containerProperties.setDefaultTimeToLiveInSeconds(-1);
        containerProperties.setUniqueKeyPolicy(uniqueKeyPolicy);

        //  Create container with 400 RU/s
        var throughputProperties = ThroughputProperties.createManualThroughput(400);
        var containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
        var container = database.getContainer(containerResponse.getProperties().getId());

        var databaseLink = getDatabaseLink(db);
        log.info("create container:{}/colls/{}, account:{}", databaseLink, cont, account);

        return container;

    }


    /**
     * Read the document container obj by dbName and collName.
     *
     * @param db   dbName
     * @param cont collName
     * @return documentCollection obj
     */
    public CosmosContainer readContainer(String db, String cont) {
        return client.getDatabase(db).getContainer(cont);
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
        return new UniqueKey(List.of(key));
    }


    static String getDatabaseLink(String db) {
        return String.format("/dbs/%s", db);
    }

    static String getCollectionLink(String db, String cont) {
        return String.format("/dbs/%s/colls/%s", db, cont);
    }

    static String getDocumentLink(String db, String cont, String id) {
        return String.format("/dbs/%s/colls/%s/docs/%s", db, cont, id);
    }

    static boolean isResourceNotFoundException(CosmosException e) {
        var message = e.getMessage() == null ? "" : e.getMessage();
        return e.getStatusCode() == 404 || message.contains("Not Found") ? true : false;
    }

    /**
     * get the default partition key
     *
     * @return default partition key
     */
    public static String getDefaultPartitionKey() {
        return "_partition";
    }

    public String getAccount() {
        return this.account;
    }


}