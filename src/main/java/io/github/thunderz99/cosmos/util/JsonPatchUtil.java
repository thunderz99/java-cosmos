package io.github.thunderz99.cosmos.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.azure.cosmos.implementation.patch.PatchOperationCore;
import com.google.common.primitives.Primitives;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.Updates;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.StringUtils;
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
        var index = 0;
        for (var _operation : operations.getPatchOperations()) {
            var operation = (PatchOperationCore<?>) _operation;
            var op = operation.getOperationType().name();
            var jsonPath = operation.getPath();
            var rawValue = operation.getResource();

            var value = getNormalizedValue(rawValue);

            var matcher = pattern.matcher(jsonPath);

            // when the patch operation is against an array field
            if (matcher.find()) {
                var arrayPath = matcher.group(1);
                var arrayIndex = Integer.parseInt(matcher.group(2));
                //TODO
                //params.add(generateOperation4Array(op, arrayPath, arrayIndex, value));
            } else { // normal patch operation

                // from /fullName/last to param001_fullName__last
                var name = ParamUtil.getParamNameFromKey(MapUtil.toPeriodKey(jsonPath), index);
                // from /fullName/last to {fullName, last}
                var path = toPostgresPath(jsonPath);

                switch (op) {
                    case "ADD", "REPLACE", "SET" -> {
                        if(subSql.isEmpty()){
                            subSql.append(TableUtil.JDOC);
                        }

                        subSql.insert(0, "jsonb_set( ");
                        subSql.append(",'%s', %s::jsonb )".formatted(path, name));
                        params.add(new CosmosSqlParameter(name, value));  // Remove leading '/' from path

                    }
                    case "REMOVE" -> {
                        if(subSql.isEmpty()){
                            subSql.append(TableUtil.JDOC);
                        }
                        subSql.append(" - " + "'%s'".formatted(path));
                        // no params added, because REMOVE does not need a value

                    }
                    case "INCREMENT" -> {
                        // TODO

                    }
                    default -> throw new UnsupportedOperationException("Unsupported JSON Patch operation: " + op);
                }
            }
            index++;
        }
        return new CosmosSqlQuerySpec(subSql.toString(), params);
    }



    /**
     * Converts a JSON Patch path to a PostgreSQL jsonb_set path.
     * <p>
     * E.g. "/a/b" -> "{a,b}"
     * </p>
     */
    static String toPostgresPath(String path) {
        Checker.check(!StringUtils.contains(path, ";"), "path should not contain semicolon ';'");
        return "{" + path.substring(1).replace("/", ",") + "}";
    }
}
