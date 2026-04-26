package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers for root data JSONB containment queries that can use the default GIN(data) index.
 */
class PGJsonbContainment {

    private PGJsonbContainment() {
    }

    static boolean canUseWholeDocumentIdEquals(String key, Object value, String operator, String selectAlias) {
        if (!isRootDataAlias(selectAlias) || !"=".equals(operator) || !(value instanceof String)) {
            return false;
        }

        var leafKey = getLeafKey(key);
        return !TableUtil.ID.equals(key) && StringUtils.endsWith(leafKey, "Id");
    }

    static boolean canUseWholeDocumentArrayContains(String key, String selectAlias) {
        return isRootDataAlias(selectAlias) && !TableUtil.ID.equals(key);
    }

    static String buildContainsQuery(String selectAlias, String paramName) {
        Checker.checkNotBlank(selectAlias, "selectAlias");
        Checker.checkNotBlank(paramName, "paramName");
        return String.format(" (%s @> %s::jsonb)", selectAlias, paramName);
    }

    static CosmosSqlParameter createContainsParameter(String key, String paramName, Object value) {
        return new CosmosSqlParameter(paramName, buildJsonbObject(key, value));
    }

    static CosmosSqlParameter createArrayContainsParameter(String key, String paramName, Object value) {
        return createContainsParameter(key, paramName, List.of(value));
    }

    static String buildJsonbObject(String key, Object value) {
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

    private static boolean isRootDataAlias(String selectAlias) {
        return TableUtil.DATA.equals(selectAlias);
    }

    private static String getLeafKey(String key) {
        if (StringUtils.isBlank(key)) {
            return "";
        }

        var parts = key.split("\\.");
        return parts[parts.length - 1];
    }
}
