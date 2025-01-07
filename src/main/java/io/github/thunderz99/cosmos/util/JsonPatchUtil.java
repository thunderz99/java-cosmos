package io.github.thunderz99.cosmos.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.azure.cosmos.implementation.patch.PatchOperationCore;
import com.google.common.primitives.Primitives;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.Updates;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.BsonDocument;

/**
 * A util to process conversions used for patch data generation and etc
 */
public class JsonPatchUtil {

    /**
     * A regex pattern for "/address/country/city/2" for json path representing an array field and index
     *
     * <p>
     *     used for mongodb
     * </p>
     */
    static final Pattern pattern = Pattern.compile("(.+)/([0-9]+)$");

    /**
     * Convert JSON Patch operations to Bson format to do an update operation
     *
     * @param operations
     */
    public static List<BsonDocument> toMongoPatchData(PatchOperations operations) {

        List<BsonDocument> updates = new ArrayList<>();
        for (var _operation : operations.getPatchOperations()) {
            var operation = (PatchOperationCore<?>) _operation;
            String op = operation.getOperationType().name();
            String path = operation.getPath();
            Object rawValue = operation.getResource();

            var value = getNormalizedValue(rawValue);

            var matcher = pattern.matcher(path);

            // when the patch operation is against an array field
            if (matcher.find()) {
                var arrayPath = matcher.group(1);
                var index = Integer.parseInt(matcher.group(2));
                updates.add(generateOperation4Array(op, arrayPath, index, value));
            } else { // normal patch operation
                switch (op) {
                    case "ADD":
                    case "REPLACE":
                    case "SET":
                        updates.add(Updates.set(MapUtil.toPeriodKey(path), value).toBsonDocument());  // Remove leading '/' from path
                        break;
                    case "REMOVE":
                        updates.add(Updates.unset(MapUtil.toPeriodKey(path)).toBsonDocument());
                        break;
                    case "INCREMENT":
                        updates.add(Updates.inc(MapUtil.toPeriodKey(path), (Number) value).toBsonDocument());
                        break;

                    default:
                        throw new UnsupportedOperationException("Unsupported JSON Patch operation: " + op);
                }
            }
        }
        return updates;
    }

    /**
     * convert rawValue(pojo / list of pojo) to normalizedValue(map / list of map) for patch operation
     *
     * @param value
     */
    public static Object getNormalizedValue(Object value) {

        if (value == null) {
            return null;
        }

        // primitive type
        if (value instanceof String || Primitives.isWrapperType(value.getClass()) || value.getClass().isPrimitive() || value.getClass().isEnum()) {
            return value;
        }

        if (value instanceof Collection<?>) {
            var collection = (Collection<?>) value;
            return collection.stream().map(item -> getNormalizedValue(item)).collect(Collectors.toList());
        }

        return JsonUtil.toMap(value);

    }

    /**
     * Generate patch operation for an array field. supports ADD / SET / REPLACE / REMOVE element with specific index.
     * <p>
     *     used for mongodb
     * </p>
     * @param op
     * @param arrayPath
     * @param index
     * @param value
     * @return
     */
    public static BsonDocument generateOperation4Array(String op, String arrayPath, int index, Object value) {

        Checker.checkNotBlank(op, "op");
        Checker.checkNotBlank(arrayPath, "arrayPath");
        Checker.check(index >= 0, "index must >= 0");

        var fieldPath = MapUtil.toPeriodKey(arrayPath);
        BsonDocument ret = null;

        switch (op) {
            case "ADD":
                // ADD: Insert at the specific position using $position
                ret = Updates.pushEach(fieldPath, List.of(value), new PushOptions().position(index)).toBsonDocument();
                break;

            case "REPLACE":
            case "SET":
                // REPLACE/SET: Use $set to replace the element at the specific index
                ret = Updates.set(fieldPath + "." + index, value).toBsonDocument();
                break;

            case "REMOVE":
                // REMOVE: to remove the element at the specific index
                throw new UnsupportedOperationException("Unsupported JSON Patch operation(remove element by index): " + op);

            default:
                throw new UnsupportedOperationException("Unsupported JSON Patch operation for array: " + op);
        }

        return ret;
    }

    /**
     * Convert JSON Patch operations to Postgres querySpec(sql and params) format to do an update operation

     * @param operations
     * @return subSql in CosmosSqlQuerySpec format, which contains sql in NamedParams and values
     */
    public static CosmosSqlQuerySpec toPostgresPatchData(PatchOperations operations) {

        var subSql = new StringBuilder();

        List<CosmosSqlParameter> params = new ArrayList<>();
        var index = new AtomicInteger(0);
        for (var _operation : operations.getPatchOperations()) {
            var operation = (PatchOperationCore<?>) _operation;
            var op = operation.getOperationType().name();
            var jsonPath = operation.getPath();
            var rawValue = operation.getResource();

            var value = getNormalizedValue(rawValue);

            // from /fullName/last to param001_fullName__last
            var paramName = ParamUtil.getParamNameFromKey(MapUtil.toPeriodKey(jsonPath), index.get());

            // from "/address/city" to ["address", "city"]
            var pathList = Arrays.stream(StringUtils.removeStart(jsonPath, "/").split("/")).toList();

            if(subSql.isEmpty()){
                subSql.append(TableUtil.DATA);
            }

            var matcher = pattern.matcher(jsonPath);
            // when the patch operation is against an array field
            if (matcher.find()) {
                if(StringUtils.equals(op, "ADD")){
                    //special case for ADD element to an array field
                    var subSql4Array = generateAddOperation4ArrayPostgres(subSql.toString(), pathList, paramName);

                    subSql = new StringBuilder(subSql4Array);
                    params.add(new CosmosSqlParameter(paramName, value));  // Remove leading '/' from path
                    index.getAndIncrement();
                    continue;
                }
            }

            // normal patch operation

            switch (op) {
                case "ADD", "REPLACE", "SET" -> {

                    /**
                     * nested json should assure the object in path exist
                     *
                     * UPDATE schema1.table1
                     * SET data = jsonb_set(
                     *              jsonb_set(
                     *                      data,
                     *                      '{address}', '{}',  -- Create the "address" object if it doesn't exist
                     *                      true
                     *               ),
                     *              '{address,city}', '"NY"'::jsonb
                     *          )
                     * WHERE id = 'id1'
                     * RETURNING *;
                     */

                    var subSqlNested = generateSubSql4NestedJson(subSql.toString(), pathList, pathList.size(), paramName);

                    subSql = new StringBuilder(subSqlNested);
                    params.add(new CosmosSqlParameter(paramName, value));

                }
                case "REMOVE" -> {
                    /**
                     * UPDATE schema1.table1
                     * SET data = jsonb_set(
                     *              data,
                     *              '{contents,address}',
                     *              (data #> '{contents,address}') - 'city'
                     *          )
                     * WHERE id = 'id1'
                     * RETURNING *;
                     */
                    var subSql4Remove = generateSubSql4Remove(subSql.toString(), pathList);
                    subSql = new StringBuilder(subSql4Remove);
                    // no params added, because REMOVE does not need a value

                }
                case "INCREMENT" -> {

                    /**
                     * jsonb_set(
                     *              data,
                     *              '{contents,age}',
                     *              CASE
                     *                  WHEN COALESCE(data #>> '{contents,age}', '0') ~ '^\d+$' THEN
                     *                      (COALESCE(data #>> '{contents,age}', '0')::bigint + 2)::text::jsonb
                     *                  WHEN COALESCE(data #>> '{contents,age}', '0') ~ '^\d+\.\d+$' THEN
                     *                      (COALESCE(data #>> '{contents,age}', '0')::numeric + 2)::text::jsonb
                     *                  ELSE
                     *                      (data #>> '{contents,age}')::jsonb
                     *              END
                     *          )
                     */
                    var subSql4Increment = generateSubSql4Increment(subSql.toString(), pathList, params, index, value);
                    subSql = new StringBuilder(subSql4Increment);

                }
                default -> throw new UnsupportedOperationException("Unsupported JSON Patch operation: " + op);
            }
            index.getAndIncrement();
        }
        return new CosmosSqlQuerySpec(subSql.toString(), params);
    }

    /**
     * Generate a subSql for postgres to do a "SET" operation for a nested json field.
     * <p>
     * input: "data", ["address", "city", "street"], level:3, "@param001_address__city__street"
     * </p>
     * <p>
     *     output:
     *     jsonb_set(
     *       jsonb_set(
     *         jsonb_set(
     *            data,
     *           '{address}',
     *           COALESCE(data #> '{address}', '{}'::jsonb),  -- Create the "/address" object if it doesn't exist
     *           true
     *         ),
     *         '{address,city}',
     *         COALESCE(data #> '{address,city}', '{}'::jsonb), -- Create the "/address/city" object if it doesn't exist
     *         true
     *       ),
     *       '{address,city,street}', '@param001_address__city__street'::jsonb
     *     )
     *
     * </p>
     *
     * @param subSql the inner sub sql. initial value if column name 'data'
     * @param keys list of nested json keys. ["address", "city", "street"] represents "/address/city/street"
     * @param level current recursive level. initial value is the size of keys. level will decrease in the inner recursion
     * @param paramName '@param001_address__city__street'
     * @return subSql for nested json
     */
    static String generateSubSql4NestedJson(String subSql, List<String> keys, int level, String paramName) {

        Checker.checkNotBlank(subSql, "subSql");
        Checker.checkNotBlank(paramName, "paramName");

        // if keys is empty, return subSql no touched
        if(CollectionUtils.isEmpty(keys)){
            return subSql;
        }

        // if level is less than or equal to 0, stop the process and return subSql
        if(level <= 0){
            return subSql;
        }

        if(level < keys.size()){
            subSql = generateSubSql4NestedJson(subSql, keys, level - 1, paramName);
            var jsonPath = "";
            for(int i = 0; i < level; i++){
                jsonPath = jsonPath + "/" + Checker.checkNotBlank(keys.get(i), "key[%d]".formatted(i));
            }
            // "/address/city" -> "'{address,city}'"
            var path = toPostgresPath(jsonPath);

            var sb = new StringBuilder(subSql);
            sb.insert(0, "jsonb_set( ");
            sb.append(",'%s', COALESCE(data #>'%s','{}'::jsonb), true)".formatted(path, path));
            return sb.toString();
        }

        if(level == keys.size()){
            subSql = generateSubSql4NestedJson(subSql, keys, level -1, paramName);
            var jsonPath = "";
            for(int i = 0; i< level; i++){
                jsonPath = jsonPath + "/" +Checker.checkNotBlank(keys.get(i), "key[%d]".formatted(i));
            }
            // "/address/city/street" -> "'{address,city,street}'"
            var path = toPostgresPath(jsonPath);
            var sb = new StringBuilder(subSql);
            sb.insert(0, "jsonb_set( ");
            sb.append(",'%s', %s::jsonb )".formatted(path, paramName));
            return sb.toString();
        }

        throw new IllegalArgumentException("level %d should be with keys' size %d".formatted(level, keys.size()));
    }

    /**
     * Generate the subSql for postgres to REMOVE an element of a specific json path.
     * <p>
     * input: "data", ["address", "city", "street"]
     * </p>
     * <p>
     *     output:
     *       jsonb_set(
     *              data,
     *              '{contents,address}',
     *              (data #> '{contents,address}') - 'city'
     *        )
     * </p>
     * @param subSql
     * @param keys
     * @return subSql for remove operation
     */
    static String generateSubSql4Remove(String subSql, List<String> keys) {
        Checker.checkNotBlank(subSql, "subSql");

        // if keys is empty, return subSql no touched
        if(CollectionUtils.isEmpty(keys)){
            return subSql;
        }

        if(keys.size() == 1){
            //return a simple remove subSql
            return "%s - '%s'".formatted(subSql, Checker.checkNotBlank(keys.get(0), "key[0]"));
        }

        var jsonPath = "";
        for(int i = 0; i < keys.size() - 1; i++){
            jsonPath = jsonPath + "/" + Checker.checkNotBlank(keys.get(i), "key[%d]".formatted(i));
        }

        // "/address/city/street" -> "'{contents,address}'"
        var path = toPostgresPath(jsonPath);
        var sb = new StringBuilder(subSql);
        sb.insert(0, "jsonb_set( ");

        var lastIndex = keys.size() - 1;
        var lastKey = Checker.checkNotBlank(keys.get(lastIndex), "key[%d]".formatted(lastIndex));
        if(!NumberUtils.isDigits(lastKey)){
            // if not digits, enclose it with ''
            // if digits, let it be untouched.
            // e.g. (data #> '{contents,IT,skills}') - 2  -- Remove the element at index 2
            lastKey = "'%s'".formatted(lastKey);
        }
        sb.append(",'%s', (data #> '%s') - %s )".formatted(path, path, lastKey));
        return sb.toString();

    }

    /**
     * Generate the subSql for postgres to ADD an element of a specific json path.
     * <p>
     *     input: "data", ["skills", 1], "@param001_skills__1"
     * </p>
     * <p>
     *     output:
     *       jsonb_set(
     *              data,
     *              '{skills}',
     *              jsonb_insert(
     *                  data->'skills',
     *                  '{1}',
     *                  '@param001_skills__1'::jsonb
     *              )
     *       )
     * </p>
     * @param subSql
     * @param keys
     * @param paramName
     * @return subSql
     */
    static String generateAddOperation4ArrayPostgres(String subSql, List<String> keys, String paramName) {

        Checker.checkNotBlank(subSql, "subSql");

        // if keys is empty, return subSql no touched
        if(CollectionUtils.isEmpty(keys)){
            return subSql;
        }

        if(keys.size() == 1){
            throw new IllegalArgumentException("ADD operation for array should have at least 2 keys(e.g. [\"skills\", 1]). given:" + keys);
        }

        var jsonPath = "";
        for(int i = 0; i < keys.size() - 1; i++){
            jsonPath = jsonPath + "/" + Checker.checkNotBlank(keys.get(i), "key[%d]".formatted(i));
        }

        // "/contents/skills/1" -> "'{contents,skills}'"
        var path = toPostgresPath(jsonPath);
        var sb = new StringBuilder(subSql);
        sb.insert(0, "jsonb_set( ");

        var lastIndex = keys.size() - 1;
        sb.append(",'%s', jsonb_insert( data #> '%s', '{%s}', %s::jsonb) )".formatted(path, path,
                Checker.checkNotBlank(keys.get(lastIndex), "key[%d]".formatted(lastIndex)),
                paramName)
        );
        return sb.toString();
    }

    /**
     * Generate the subSql for postgres to INCREMENT an element of a specific json path.
     * <p>
     *     input: "data", ["score"], "@param001_score"
     * </p>
     * <p>
     *     output:
     *     jsonb_set(
     *              data,
     *              '{contents,age}',
     *              CASE
     *                  WHEN COALESCE(data #>> '{contents,age}', '0') ~ '^\d+$' THEN  -- Check if it's a Long (integer)
     *                      (COALESCE(data #>> '{contents,age}', '0')::bigint + 2)::text::jsonb
     *                  WHEN COALESCE(data #>> '{contents,age}', '0') ~ '^\d+\.\d+$' THEN  -- Check if it's a Double (numeric)
     *                      (COALESCE(data #>> '{contents,age}', '0')::numeric + 2)::text::jsonb
     *                  ELSE
     *                      (data #>> '{contents,age}')::jsonb  -- Default to unchanged for non-numeric
     *              END
     *          )
     *
     * </p>
     * @param subSql
     * @param keys
     * @param params
     * @param paramIndex
     * @return subSql for increment
     */
    static String generateSubSql4Increment(String subSql, List<String> keys, List<CosmosSqlParameter> params, AtomicInteger paramIndex, Object value) {

        Checker.checkNotBlank(subSql, "subSql");

        // if keys is empty, return subSql no touched
        if(CollectionUtils.isEmpty(keys)){
            return subSql;
        }

        var jsonPath = "";
        for(int i = 0; i < keys.size(); i++){
            jsonPath = jsonPath + "/" + Checker.checkNotBlank(keys.get(i), "key[%d]".formatted(i));
        }

        // "/contents/skills/1" -> "'{contents,skills}'"
        var path = toPostgresPath(jsonPath);
        var sb = new StringBuilder(subSql);
        sb.insert(0, "jsonb_set( ");

        var paramName1 = ParamUtil.getParamNameFromKey(MapUtil.toPeriodKey(jsonPath), paramIndex.getAndIncrement());
        var paramName2 = ParamUtil.getParamNameFromKey(MapUtil.toPeriodKey(jsonPath), paramIndex.get());

        params.add(new CosmosSqlParameter(paramName1, value));
        params.add(new CosmosSqlParameter(paramName2, value));

        var block =
         """
             ,
             '%s',
             CASE
                 WHEN COALESCE(data #>> '%s', '0') ~ '^\\d+$' THEN  -- Check if it's a Long (integer)
                     (COALESCE(data #>> '%s', '0')::bigint + %s::int)::text::jsonb
                 WHEN COALESCE(data #>> '%s', '0') ~ '^\\d+\\.\\d+$' THEN  -- Check if it's a Double (numeric)
                     (COALESCE(data #>> '%s', '0')::numeric + %s::int)::text::jsonb
                 ELSE
                     (data #> '%s')  -- Default to unchanged for non-numeric
             END
         )
         """;
        sb.append(block.formatted(path, path, path, paramName1,path, path, paramName2, path));
        return sb.toString();
    }


    /**
     * Converts a JSON Patch path to a PostgreSQL jsonb_set path.
     * <p>
     * E.g. "/a/b" -> "{a,b}". "/score" -> "{score}"
     * </p>
     */
    static String toPostgresPath(String path) {

        if(StringUtils.isEmpty(path)){
            return path;
        }
        Checker.check(!StringUtils.contains(path, ";"), "path should not contain semicolon ';'");

        // Remove leading '/'
        path = StringUtils.removeStart(path, "/");

        return "{" + path.replace("/", ",") + "}";
    }
}
