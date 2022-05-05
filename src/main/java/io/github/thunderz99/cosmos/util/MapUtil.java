package io.github.thunderz99.cosmos.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * A simple util operate maps. e.g. convert a nested map to a flat map for partial update
 */
public class MapUtil {


    /**
     * Convert a traditional java map to a "/key1/key2/key3" : value format map. Used by v4 patch method.
     *
     * <p>
     * {@code
     * // input:
     * {
     *     "id": "ID001",
     *     "contents": {
     *         "name": "Tom",
     *         "age": 25
     *     }
     * }
     *
     * // outpput:
     * {
     *     "/id": "ID001",
     *     "/contents/name": "Tom",
     *     "/contents/age": 25
     * }
     *
     *
     * </p>
     *
     * @param map
     * @return
     */
    public static Map<String, Object> toFlatMap(Map<String, ? extends Object> map) {
        return toFlatMap("", map);
    }

    static Map<String, Object> toFlatMap(String baseKey, Map<String, ? extends Object> map) {

        if (map == null) {
            return null;
        }

        if (map.isEmpty()) {
            return new LinkedHashMap<>();
        }

        var ret = new LinkedHashMap<String, Object>();

        for (var entry : map.entrySet()) {

            var key = entry.getKey();

            if (StringUtils.isEmpty(key)) {
                // we do not support empty key at present
                continue;
            }

            var value = entry.getValue();
            var subKey = baseKey + "/" + key;

            if (value instanceof Map) {
                var subMap = (Map<String, Object>) value;
                var flatSubMap = toFlatMap(subKey, subMap);
                ret.putAll(flatSubMap);
            } else {
                ret.put(subKey, value);
            }
        }

        return ret;
    }


}
