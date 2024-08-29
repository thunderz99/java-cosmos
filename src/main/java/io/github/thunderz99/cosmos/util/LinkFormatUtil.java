package io.github.thunderz99.cosmos.util;

/**
 * A util class to generate link string for database, collection, document in order to log there info easier to understand
 */
public class LinkFormatUtil {

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

}
