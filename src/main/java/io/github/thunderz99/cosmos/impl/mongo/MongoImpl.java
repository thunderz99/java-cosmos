package io.github.thunderz99.cosmos.impl.mongo;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosContainerResponse;
import io.github.thunderz99.cosmos.dto.UniqueKeyPolicy;
import io.github.thunderz99.cosmos.util.Checker;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * class that represent a cosmos account
 *
 * <pre>
 * Usage: var cosmos = new
 * MongoImpl("mongodb://localhost:27017");
 * var db = cosmos.getDatabase("Database1");
 *
 * //Then use db to do CRUD / query db.upsert("Users", user);
 * </pre>
 *
 */
public class MongoImpl implements Cosmos {

    private static Logger log = LoggerFactory.getLogger(MongoImpl.class);

    MongoClient client;

    String account;

    public MongoImpl(String connectionString) {

        var mongoClient = MongoClients.create(connectionString);
        log.info("mongodb Connection successful: " + preFlightChecks(mongoClient));
        log.info("mongodb Print list of databases:");

        this.client = mongoClient;
        this.account = extractAccountName(connectionString);

        var databases = this.client.listDatabases().into(new ArrayList<>());
        databases.forEach(db -> log.info(db.toJson()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.client.close();
        }));

    }

    /**
     * do a Ping to mongo
     * @param mongoClient
     * @return
     */
    static boolean preFlightChecks(MongoClient mongoClient) {
        Document pingCommand = new Document("ping", 1);
        Document response = mongoClient.getDatabase("admin").runCommand(pingCommand);
        log.info("mongodb: {ping: 1} cmd result: " + response.toJson(JsonWriterSettings.builder().indent(true).build()));
        return response.get("ok", Number.class).intValue() == 1;
    }


    /**
     * Get a CosmosDatabase object by name
     *
     * @param db database name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
        return new MongoDatabaseImpl(this, db);
    }


    /**
     * extract the MongoDB 's account(cluster) name from the connectionString
     *
     * @param connectionString the connectionString for MongoDB
     * @return account name
     */
    static String extractAccountName(String connectionString) {

        var endpoint = "";

        if(StringUtils.contains(connectionString, "mongodb://")){
            endpoint = StringUtils.replace(connectionString, "mongodb://", "https://");
        } else if(StringUtils.contains(connectionString, "mongodb+srv://")){
            endpoint = StringUtils.replace(connectionString, "mongodb://", "https://");
        }

        if(StringUtils.isEmpty(endpoint)){
            throw new IllegalArgumentException("connectionString not valid for mongodb: " + connectionString);
        }

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

        var mongoDatabase = this.client.getDatabase(db);
        log.info("created database:{}, account:{}", db, account);

        if (StringUtils.isBlank(coll)) {
            //do nothing
        } else {
            mongoDatabase.getCollection(coll);
            //mongoDatabase.createCollection(coll);
        }

        // TODO
        // deal with uniqueKeyPolicy

        return new MongoDatabaseImpl(this, db);
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
        var mongoDatabase = this.client.getDatabase(db);
        try {
            mongoDatabase.drop();
        } catch (MongoException me) {
            if (isResourceNotFoundException(me)) {
                log.info("delete Database not exist. Ignored:{}, account:{}", getDatabaseLink(db), this.account);
            } else {
                throw new CosmosException(me);
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

        var mongoDatabase = this.client.getDatabase(db);
        var collection = mongoDatabase.getCollection(coll);
        try {
            collection.drop();
        } catch (MongoException me) {
            // If not exist
            if (isResourceNotFoundException(me)) {
                log.info("delete Collection not exist. Ignored:{}, account:{}", getCollectionLink(db, coll), this.account);
            } else {
                // Throw any other Exception
                throw new CosmosException(me);
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
            var collection =  database.getCollection(coll);
            log.info("indexes:" + collection.listIndexes());
            return new io.github.thunderz99.cosmos.dto.CosmosContainerResponse();
        } catch (MongoException me) {
            if (isResourceNotFoundException(me)) {
                return null;
            }
            throw new CosmosException(me);
        }
    }


    /**
     * Get Official MongoClient instance
     *
     * @return official mongodb client
     */
    public MongoClient getClient() {
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

    public static boolean isResourceNotFoundException(Exception e) {
        if (e instanceof MongoException) {
            var me = (MongoException) e;
            return me.getCode() == 404;
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