package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A util class for building root data JSONB containment queries that can use the default GIN(data) index.
 */
public class PGJsonbContainmentUtil {

    /**
     * Prevent instantiation because this class only contains static helper methods.
     */
    private PGJsonbContainmentUtil() {
    }

    /**
     * Checks whether an equality predicate can be rewritten as a whole-document JSONB containment predicate.
     *
     * @param key         filter key such as targetId
     * @param value       filter value
     * @param operator    filter operator
     * @param selectAlias SQL alias for the JSONB document
     * @return true if the predicate can be generated as data @> ...::jsonb
     */
    public static boolean canUseWholeDocumentIdEquals(String key, Object value, String operator, String selectAlias) {
        if (!isRootDataAlias(selectAlias) || !"=".equals(operator) || !(value instanceof String)) {
            return false;
        }

        var leafKey = getLeafKey(key);
        return !TableUtil.ID.equals(key) && StringUtils.endsWith(leafKey, "Id");
    }

    /**
     * Checks whether an ARRAY_CONTAINS predicate can be rewritten as a whole-document JSONB containment predicate.
     *
     * @param key         filter key such as targetIdList
     * @param selectAlias SQL alias for the JSONB document
     * @return true if the predicate can be generated as data @> ...::jsonb
     */
    public static boolean canUseWholeDocumentArrayContains(String key, String selectAlias) {
        return isRootDataAlias(selectAlias) && !TableUtil.ID.equals(key);
    }

    /**
     * Builds a JSONB containment SQL fragment.
     *
     * @param selectAlias SQL alias for the JSONB document
     * @param paramName   named SQL parameter containing JSON text
     * @return SQL fragment such as " (data @> @param::jsonb)"
     */
    public static String buildContainsQuery(String selectAlias, String paramName) {
        Checker.checkNotBlank(selectAlias, "selectAlias");
        Checker.checkNotBlank(paramName, "paramName");
        return String.format(" (%s @> %s::jsonb)", selectAlias, paramName);
    }

    /**
     * Creates a parameter whose value is a JSON object used by the JSONB containment operator.
     *
     * @param key       filter key in dot notation
     * @param paramName named SQL parameter
     * @param value     value to put at the filter key
     * @return SQL parameter containing JSON text
     */
    public static CosmosSqlParameter createContainsParameter(String key, String paramName, Object value) {
        return new CosmosSqlParameter(paramName, buildJsonbObject(key, value));
    }

    /**
     * Creates a parameter for array containment, wrapping the value in a single-item JSON array.
     *
     * @param key       filter key in dot notation
     * @param paramName named SQL parameter
     * @param value     array item value
     * @return SQL parameter containing JSON text
     */
    public static CosmosSqlParameter createArrayContainsParameter(String key, String paramName, Object value) {
        return createContainsParameter(key, paramName, List.of(value));
    }

    /**
     * Builds a JSON object string from a dot-notated key and value.
     *
     * @param key   filter key in dot notation
     * @param value value to put at the filter key
     * @return JSON object string
     */
    public static String buildJsonbObject(String key, Object value) {
        Checker.checkNotBlank(key, "key");

        Object current = value;
        var parts = key.split("\\.");
        for (var i = parts.length - 1; i >= 0; i--) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(parts[i], current);
            current = map;
        }

        return JsonUtil.toJson(current);
    }

    /**
     * Checks whether the select alias points to the root data JSONB column.
     *
     * @param selectAlias SQL alias for the JSONB document
     * @return true if the alias is the root data column alias
     */
    private static boolean isRootDataAlias(String selectAlias) {
        return TableUtil.DATA.equals(selectAlias);
    }

    /**
     * Extracts the last segment from a dot-notated key.
     *
     * @param key filter key in dot notation
     * @return leaf key, or an empty string for blank input
     */
    private static String getLeafKey(String key) {
        if (StringUtils.isBlank(key)) {
            return "";
        }

        var parts = key.split("\\.");
        return parts[parts.length - 1];
    }
}
