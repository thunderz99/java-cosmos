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
     * }
     *
     * </p>
     *
     * @param map map to convert
     * @return map flattened
     */
    public static Map<String, Object> toFlatMap(Map<String, ? extends Object> map) {
        return toFlatMap("", map, true);
    }


    /**
     * Convert a traditional java map to a "key1.key2.key3" : value format map. Used by updatePartial in mongodb method.
     *
     * <p>
     * {@code
     * // input:
     * {
     * "id": "ID001",
     * "contents": {
     * "name": "Tom",
     * "age": 25
     * }
     * }
     * <p>
     * // outpput:
     * {
     * "/id": "ID001",
     * "/contents/name": "Tom",
     * "/contents/age": 25
     * }
     * }
     *
     * </p>
     *
     * @param map map to convert
     * @return map flattened
     */
    public static Map<String, Object> toFlatMapWithPeriod(Map<String, ? extends Object> map) {
        return toFlatMap("", map, false);
    }


    static Map<String, Object> toFlatMap(String baseKey, Map<String, ? extends Object> map, boolean useSlash) {

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
                // this is a mongodb constraint, which cannot update empty key for a document
                continue;
            }

            var value = entry.getValue();
            var subKey = "";

            if (useSlash) {
                subKey = baseKey + "/" + key;
            } else {
                //use period
                subKey = StringUtils.isEmpty(baseKey) ? key : baseKey + "." + key;
            }

            if (value instanceof Map) {
                var subMap = (Map<String, Object>) value;
                var flatSubMap = toFlatMap(subKey, subMap, useSlash);
                ret.putAll(flatSubMap);
            } else {
                ret.put(subKey, value);
            }
        }

        return ret;
    }


    /**
     * Convert "/address/country/city" JSON Patch format to "address.country.city" mongo format
     *
     * @param path
     * @return converted key
     */
    public static String toPeriodKey(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be null or empty");
        }
        // remove the heading slash and replace / to .
        return path.startsWith("/") ? path.substring(1).replace("/", ".")
                : path.replace("/", ".");
    }

    /**
     * like Object.assign(m1, m2) in javascript, but support nested merge.
     *
     * @param m1
     * @param m2
     * @return map after merge
     */
    public static Map<String, Object> merge(Map<String, Object> m1, Map<String, Object> m2) {

        for (var entry : m1.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();

            var value2 = m2.get(key);

            // do nested merge
            if (value != null && value instanceof Map<?, ?> && value2 != null && value2 instanceof Map<?, ?>) {
                var subMap1 = (Map<String, Object>) value;
                var subMap2 = (Map<String, Object>) value2;

                subMap1 = merge(subMap1, subMap2);
                m2.put(key, subMap1);
            }

        }


        m1.putAll(m2);
        return m1;
    }

    /**
     * Check if a map is immutable.
     *
     * Note: This method is not 100% reliable, but it should work for most cases.
     *
     * @param map
     * @return true/false
     */
    public static boolean isImmutableMap(Map<?, ?> map) {
        if(map == null){
            return false;
        }
        var className = map.getClass().getName();
        return className.contains("Immutable") || className.contains("Unmodifiable") || className.contains("SingletonMap");
    }

    /**
     * Returns true if map contains any empty key "" at any depth.
     *
     * @param map map to check
     * @return true/false
     */
    public static boolean containsEmptyKeyDeep(Map<String, ?> map) {
        if (map == null) return false;
        for (var e : map.entrySet()) {
            var k = e.getKey();
            if (k == null || k.isEmpty()) return true;
            var v = e.getValue();
            if (v instanceof Map<?, ?> sub && containsEmptyKeyDeep((Map<String, ?>) sub)) {
                return true;
            }
        }
        return false;
    }

}
