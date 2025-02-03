package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.util.Checker;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

        return getFormattedKeyWithAlias(key, "data", "");
    }

    /**
     * Instead of data.key, return data->'key' or data->'address'->'city' for query(for json, with "->" instead of "->>")
     *
     * @param key filter's key
     * @param selectAlias typically "data" (or "j1" "s1" for cond.join query)
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
     * @param selectAlias typically "data" (or "tableAlias.data")
     * @param value used to determine the cast type
     * @return an expression like "(data->'address'->>'city')::text"
     */
    public static String getFormattedKeyWithAlias(String key, String selectAlias, Object value) {

        // Validate alias (you can skip if your environment guarantees non-blank)
        Checker.checkNotBlank(selectAlias, "selectAlias");

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
        } else if (value instanceof Integer) {
            return "(" + basePath + ")::int";
        } else if (value instanceof Long) {
            return "(" + basePath + ")::bigint";
        } else if (value instanceof Float || value instanceof Double) {
            return "(" + basePath + ")::float8"; // float8 = double precision
        } else if (value instanceof java.math.BigDecimal) {
            return "(" + basePath + ")::numeric";
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
}
