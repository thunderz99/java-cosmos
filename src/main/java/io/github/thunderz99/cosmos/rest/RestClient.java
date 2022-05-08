package io.github.thunderz99.cosmos.rest;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.collect.Maps;
import com.microsoft.azure.documentdb.internal.Utils;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.ConnectionStringUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A client using rest api to provider methods accessing cosmos db, which v2 sdk cannot provide(e.g. patch methods)
 *
 * <p>
 *   In the future, this client will be replaced by v4 sdk, when v4 is stable.
 * </p>
 */
public class RestClient {

    /**
     * cosmos account endpoint uri
     */
    String endpoint;

    /**
     * cosmos account master key
     */
    private String key;

    /**
     * Message Authentication Code
     */
    private Mac macInstance;

    /**
     * cosmos rest api contentTypeMap
     *
     */
    static Map<String, String> contentTypeMap = Maps.newHashMap();

    static {

        contentTypeMap.put("GET", "application/json");
        contentTypeMap.put("POST", "application/json");
        contentTypeMap.put("PUT", "application/json");
        contentTypeMap.put("DELETE", "application/json");
        contentTypeMap.put("PATCH", "application/query+json");

        // To close unirest gracefully when jvm shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Unirest.shutDown();
        }));
    }


    public RestClient(String connectionString){
        var pair = ConnectionStringUtil.parseConnectionString(connectionString);
        this.endpoint = StringUtils.removeEnd(pair.getLeft(), "/");
        this.key = pair.getRight();

        Checker.checkNotBlank(this.key, "connectionString key");

        byte[] masterKeyDecodedBytes = Base64.decodeBase64(this.key.getBytes());
        SecretKeySpec signingKey = new SecretKeySpec(masterKeyDecodedBytes, "HMACSHA256");

        try {
            this.macInstance = Mac.getInstance("HMACSHA256");
            this.macInstance.init(signingKey);
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public CosmosDocument increment(String db, String coll, String id, String path, String partition){

        var resourceLink = StringUtils.removeStart(Cosmos.getDocumentLink(db, coll, id), "/");

        var verb = "PATCH";

        var contentType = buildContentType(verb);
        var partitionHeaders = buildPartitionHeader(partition);
        var authHeaders = buildAuthHeaders(key, verb, "docs", resourceLink);

        var url = endpoint + "/" + resourceLink;

        var body = PatchBody.operation(new Operation("Increment", path, 1 ));

        var result = Unirest.patch(url)
                .headers(contentType) // content type
                .headers(partitionHeaders) // partition
                .headers(authHeaders) // auth header
                .body(JsonUtil.toJson(body)) // body in json
                .asJson();

        var data = checkAndGetData(result);
        return new CosmosDocument(data);
    }

    /**
     * A method to smoke test whether auth and rest api work
     * @param db database name
     * @return map object represent a cosmos database
     */
    Map<String, Object> getDatabase(String db){

        var resourceLink = StringUtils.removeStart(Cosmos.getDatabaseLink(db), "/");

        Map<String, String> authHeaders = buildAuthHeaders(key, "GET", "dbs", resourceLink);

        var url = endpoint + "/" + resourceLink;

        var result = Unirest.get(url).headers(authHeaders).asJson();

        return checkAndGetData(result);
    }

    /**
     * Check the http result and get data. If http status is not 2xx, throw CosmosException
     * @param result
     * @return
     */
    static Map<String, Object> checkAndGetData(HttpResponse<JsonNode> result) {
        var data = result.getBody().getObject().toMap();
        if(result.getStatus() >= 300){
            if(result.getStatus() >= 300){
                throw new CosmosException(result.getStatus(), MapUtils.getString(data, "code"), MapUtils.getString(data, "message"));
            }
        }
        return data;
    }

    /**
     * build the partition header for rest api.
     * @param partition
     * @return
     */
    static Map<String, String> buildPartitionHeader(String partition) {
        return Map.of("x-ms-documentdb-partitionkey", String.format("[\"%s\"]", partition));
    }

    /**
     * Build the contentType header for rest api. Default is "application/json"
     * @param verb
     * @return
     */
    static Map<String, String> buildContentType(String verb){
        return Map.of("Content-Type", contentTypeMap.getOrDefault(verb, "application/json"));
    }

    /**
     * build http headers for rest api auth
     * @param key master key
     * @param verb GET/POST/PUT/DELETE/PATCH
     * @param resourceType dbs/colls/docs
     * @param resourceLink dbs/{databaseId}/colls/{containerId}/docs/{docId}
     * @return auth headers map
     */
    Map<String, String> buildAuthHeaders(String key, String verb, String resourceType, String resourceLink) {

        Map<String, String> headers = Maps.newHashMap();
        headers.put("Accept", "application/json");
        headers.put("x-ms-version", "2018-12-31");
        var date = Utils.getCurrentTimeGMT();
        headers.put("x-ms-date", date);
        headers.put("Authorization", buildAuthToken(key, verb, resourceType, resourceLink, date));

        return headers;
    }

    /**
     * build the authToken required by cosmos rest api
     * <p>
     * @see <a href="https://docs.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources#authorization-header">auth header</a>
     * </p>
     * @param key masterKey
     * @param verb GET/POST/PUT/DELETE/PATCH
     * @param resourceType dbs/colls/docs
     * @param resourceLink dbs/{databaseId}/colls/{containerId}/docs/{docId}
     * @param date  RFC 7231 Date/Time Formats), e.g. "Tue, 01 Nov 1994 08:12:31 GMT".
     * @return authToken. e.g. "type=master&ver=1.0&sig={hashSignature}"
     */
    String buildAuthToken(String key, String verb, String resourceType, String resourceLink, String date) {
        String body = String.format("%s\n%s\n%s\n%s\n\n", verb.toLowerCase(), resourceType, resourceLink, date.toLowerCase());
        Mac mac = null;

        try {
            mac = (Mac)this.macInstance.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }

        byte[] digest = mac.doFinal(body.getBytes());
        String hashSignature = Utils.encodeBase64String(digest);
        return "type=master&ver=1.0&sig=" + hashSignature;
    }
}
