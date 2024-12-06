package io.github.thunderz99.cosmos.impl.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosBuilder;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosContainerResponse;
import io.github.thunderz99.cosmos.dto.UniqueKeyPolicy;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.TableUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Objects;

/***
 * class that represent a cosmos account
 *
 * <pre>
 * Usage: var cosmos = new
 * PostgresImpl("postgres://localhost:5432/test?sslmode=disable");
 * var db = cosmos.getDatabase("test");
 *
 * //Then use db to do CRUD / query db.upsert("Users", user);
 * </pre>
 *
 */
public class PostgresImpl implements Cosmos {

    private static final Logger log = LoggerFactory.getLogger(PostgresImpl.class);

    HikariDataSource dataSource;

    String account;

    /**
     * whether automatically add "_expireAt" field based on "ttl" field
     */
    boolean expireAtEnabled = false;

    /**
     * whether automatically add "_etag" field for optimistic lock
     */
    boolean etagEnabled = false;


    public PostgresImpl(String connectionString) {
        this(connectionString, false, false);
    }

    public PostgresImpl(String connectionString, boolean expireAtEnabled, boolean etagEnabled) {

        var pair = parseToHikariConfig(connectionString);
        var config = pair.getLeft();
        this.account = pair.getRight();

        this.dataSource = new HikariDataSource(config);

        preFlightChecks(this.dataSource);

        this.expireAtEnabled = expireAtEnabled;
        this.etagEnabled = etagEnabled;

        Runtime.getRuntime().addShutdownHook(new Thread(this::closeClient));

    }

    /**
     * parse connectionString("postgres://user:pass@localhost:5432/database1") to HikariConfig
     * @param connectionString postgres connectionString
     * @return pair of HikariConfig and account(hostName)
     */
    static Pair<HikariConfig, String> parseToHikariConfig(String connectionString) {

        Checker.checkNotBlank(connectionString, "connectionString");
        var config = new HikariConfig();
        URI uri;
        try {
            // use URI to parse connectionString
            uri = new URI(connectionString
                    .replace("jdbc:postgresql://", "http://") // support both "jdbc:postgresql:"
                    .replace("postgresql://", "http://") // "and postgresql:"
            ); // Temporarily replace the schema

        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("connectionString not valid for postgres: " + connectionString, e);
        }

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d%s",
                uri.getHost(), uri.getPort(), uri.getRawPath());

        if(StringUtils.isNotEmpty(uri.getRawQuery())) {
            jdbcUrl += "?" + uri.getRawQuery();
        }
        config.setJdbcUrl(jdbcUrl);

        var userInfo = uri.getUserInfo();
        if(StringUtils.isNotEmpty(userInfo)){
            var infos = uri.getUserInfo().split(":");
            config.setUsername(infos[0]);
            if(infos.length > 1){
                config.setPassword(infos[1]);
            }
        }
        config.setDriverClassName("org.postgresql.Driver");

        var account = uri.getHost();

        return Pair.of(config, account);
    }

    /**
     * do a Ping to postgres
     */
    static void preFlightChecks(HikariDataSource dataSource) {
        try (var conn = dataSource.getConnection()) {
            if (conn.isValid(2)) { // Timeout of 2 seconds
                log.info("Ping successful: postgres database is reachable.");
            } else {
                log.error("Ping failed: Connection is invalid.");
            }
        } catch (SQLException e) {
            throw new CosmosException(500, "500", "Ping failed for postgres db", e);
        }
    }


    /**
     * Get a CosmosDatabase object by name
     *
     * @param db database name
     * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        return new PostgresDatabaseImpl(this, db);
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

        if(StringUtils.isEmpty(db)){
            return new PostgresDatabaseImpl(this, db);
        }

        db = TableUtil.checkAndNormalizeValidEntityName(db);

        // db
        // we map db to postgresql's schema
        try(var conn = this.dataSource.getConnection()) {

            var sql = "CREATE SCHEMA " + db;
            try (var stmt = conn.createStatement()) {
                stmt.execute(sql);
            }

            // do nothing for coll
            // we do not need to create a collection for postgres, because we use partition to represent a table in postgres.
            // collection will be considered as a tenantId column in every table.
            return new PostgresDatabaseImpl(this, db);

        } catch (SQLException e) {
            throw new CosmosException(500, "500", "createIfNotExist failed for db: " + db, e);
        }
    }

    /**
     * Check if db is a valid name. If not, throws IllegalArgumentException
     * @param db
     */
    static void checkValidDbName(String db) {
        TableUtil.checkAndNormalizeValidEntityName(db);
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

        checkValidDbName(db);

        if(Objects.isNull(dataSource)){
            // do nothing
            return;
        }
        // db
        // we map db to postgresql's schema
        try(var conn = dataSource.getConnection()) {

            var sql = "DROP SCHEMA " + db + " CASCADE";
            try (var stmt = conn.createStatement()) {
                stmt.execute(sql);
            }

        } catch (SQLException e) {
            throw new CosmosException(500, "500", "createIfNotExist failed for db: " + db, e);
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
        // do nothing
        // we do not need to create a collection for postgres, because we use partition to represent a table in postgres.
        // collection will be considered as a tenantId column in every table.
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
        // return a minimum response;
        return new CosmosContainerResponse(coll);
    }


    /**
     * Get Official MongoClient instance
     *
     * @return official mongodb client
     */
    public HikariDataSource getDataSource() {
        return this.dataSource;
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

    public String getAccount() throws CosmosException {
        return account;
    }

    @Override
    public String getDatabaseType() {
        return CosmosBuilder.MONGODB;
    }

    /**
     * Close the internal database client safely
     */
    @Override
    public void closeClient() {
        this.getDataSource().close();
    }
    
}