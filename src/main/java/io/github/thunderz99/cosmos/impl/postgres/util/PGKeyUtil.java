package io.github.thunderz99.cosmos.impl.postgres.util;

import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.util.Checker;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A util class for postgres data(JSONB) column's json key(e.g. data->>'age', data->'address'->>'city')
 */
public class PGKeyUtil {

    /**
     * Instead of data.key, return data->'key' or data->'address'->>'city' for query
     *
     * @param key filter's key
     * @return formatted filter's key data->'key1'->>'key2'
     */
    public static String getFormattedKey(String key) {

        return getFormattedKeyWithAlias(key, TableUtil.DATA, "");
    }

    /**
     * Instead of data.key, return data->'key' or data->'address'->'city' for query(for json, with "->" instead of "->>")
     *
     * @param key filter's key
     * @param selectAlias typically "data" (or "j1" "s1" for cond.join query) (or empty for afterAggregation)
     * @return formatted filter's key data->'key1'->'key2'
     */
    public static String getFormattedKey4JsonWithAlias(String key, String selectAlias) {
        return getFormattedKeyWithAlias(key, selectAlias, List.of());
    }

    /**
     * Instead of data.key, returns something like:
     *  - (data->>'age')::int
     *  - data->'address'->>'city'
     *  - data->'some'->'nested'->>'field'
     *
     * @param key e.g. "address.city" or "age"
     * @param selectAlias typically "data" (or "j1" "s1" for cond.join query) (or empty for afterAggregation)
     * @param value used to determine the cast type
     * @return an expression like "(data->'address'->>'city')::text"
     */
    public static String getFormattedKeyWithAlias(String key, String selectAlias, Object value) {

        if(StringUtils.isBlank(selectAlias)){
            // for afterAggregation. see QueryContext.afterAggregation for details
            return StringUtils.contains(key, "\"") ? key : "\"" + key + "\"";
        }

        // If the user didn't provide a key, just return the alias
        if (StringUtils.isBlank(key)) {
            return selectAlias;
        }

        // Split on "." to handle nested fields
        var parts = key.split("\\.");

        // Build all but the last level using "->"
        // Example: for "address.city.country",
        // the intermediate path is data->'address'->'city'
        var sb = new StringBuilder(selectAlias);
        for (int i = 0; i < parts.length - 1; i++) {
            sb.append("->'").append(parts[i]).append("'");
        }

        // The last part will use ->>
        var lastPart = parts[parts.length - 1];
        // If there's at least one part, append ->>'lastPart'
        // Otherwise (in the weird case parts.length==0), you'd just return the alias
        sb.append("->>'").append(lastPart).append("'");

        // Now sb is something like "data->'address'->'city'->>'country'"
        // We'll wrap it with parentheses and add a cast if needed
        var basePath = sb.toString();

        if (value instanceof Boolean) {
            return "(" + basePath + ")::boolean";
        } else if (value instanceof Integer
                || value instanceof Double
                || value instanceof Long
                || value instanceof Float
                || value instanceof java.math.BigDecimal )  {
            // cast to numeric. if empty string, return null
            return "NULLIF(" + basePath + ",'')::numeric";
        } else if (value instanceof String) {
            return basePath;
        } else if (value instanceof Collection<?>) {
            // treat it as json
            return basePath.replace("->>", "->");
        } else {
            // Fallback: treat it as String (no cast at all, depending on your preference)
            return basePath;
        }
    }

    /**
     * convert address.city.street to $."address"."city"."street"
     *
     * @param key address.city.street
     * @return $."address"."city"."street"
     */
    public static String getJsonbPathKey(String key) {

        Checker.checkNotBlank(key, "key");

        var parts = key.split("\\.");
        return Arrays.stream(parts).map(s -> "\""+s+"\"").collect(Collectors.joining(".", "$.", ""));
    }

    /**
     * Get the key format for jsonb path operators.
     * <pre>
     *     INPUT:
     *     joinPart: area.city.street.rooms
     *     key: area.city.street.rooms.no
     *     OUTPUT: ($.area.city.street.rooms[*], @.no)
     *
     *     INPUT:
     *     joinPart: room*no-01
     *     key: room*no-01.area
     *     OUTPUT: ($."room*no-01.area"[*], @.area)
     *
     * </pre>
     * @param joinPart the key to the join array
     * @param key the key to the field
     * @return (joinPart, key) in jsonb path format
     */
    public static Pair<String, String> getJsonbPathKey(String joinPart, String key) {

        Checker.checkNotBlank(joinPart, "joinPart");
        Checker.checkNotBlank(key, "key");

        var remainedKey = StringUtils.removeStart(key, joinPart + ".");

        var joinPartArray = joinPart.split("\\.");
        var keyArray = remainedKey.split("\\.");

        var joinPartJsonbPath = Arrays.stream(joinPartArray).map(s -> "\""+s+"\"").collect(Collectors.joining(".", "$.", "[*]"));
        var keyJsonbPath = Arrays.stream(keyArray).map(s -> "\""+s+"\"").collect(Collectors.joining(".", "@.", ""));
        return Pair.of(joinPartJsonbPath, keyJsonbPath);

    }

    /**
     * generate a formatted key for aggregate
     * @param key key in dot format (address.city.street)
     * @param function function part. e.g. COUNT(address.city.street)
     * @return formatted key
     */
    public static String getFormattedKey4Aggregate(String key, String function) {

        if(StringUtils.startsWithIgnoreCase(function, "COUNT(")){
            // COUNT will not need calculation in numeric, just use data->>'key'
            return getFormattedKeyWithAlias(key, TableUtil.DATA, "");
        }

        // for others (SUM, MIN, MAX, AVG, etc.)
        // return (data->>'key')::numeric
        return getFormattedKeyWithAlias(key, TableUtil.DATA, 0);

    }
}
