package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.util.Checker;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;

/**
 * A util class for postgres data(JSONB) column's json key(e.g. data->>'age', data->'address'->>'city')
 */
public class PGKeyUtil {


    /**
     * Instead of data.key, return data->'key' or data->'address'->'city' for query
     *
     * @param key filter's key
     * @return formatted filter's key data->'key1'->'key2'
     */
    public static String getFormattedKey(String key) {

        return getFormattedKeyWithAlias(key, "data", "");
    }

    /**
     * Instead of data.key, return data->'age'::int or data->'address'->'city' for query
     *
     * <p>
     *     If value is an Integer, "::int" will be added to the end of the formatted key, in order to correctly extract the data in JSONB
     * </p>
     *
     * @param key filter's key
     * @param value filter's value
     * @return formatted filter's key data->'key1'->'key2'
     */
    public static String getFormattedKey(String key, Object value) {

        return getFormattedKeyWithAlias(key, "data", value);
    }

    /**
     * Instead of data.key, returns something like:
     *  - (data->>'age')::int
     *  - data->'address'->>'city'
     *  - data->'some'->'nested'->>'field'
     *
     * @param key e.g. "address.city" or "age"
     * @param collectionAlias typically "data" (or "tableAlias.data")
     * @param value used to determine the cast type
     * @return an expression like "(data->'address'->>'city')::text"
     */
    public static String getFormattedKeyWithAlias(String key, String collectionAlias, Object value) {

        // Validate alias (you can skip if your environment guarantees non-blank)
        Checker.checkNotBlank(collectionAlias, "collectionAlias");

        // If the user didn't provide a key, just return the alias
        if (StringUtils.isBlank(key)) {
            return collectionAlias;
        }

        // Split on "." to handle nested fields
        var parts = key.split("\\.");

        // Build all but the last level using "->"
        // Example: for "address.city.country",
        // the intermediate path is data->'address'->'city'
        var sb = new StringBuilder(collectionAlias);
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
}
