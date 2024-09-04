package io.github.thunderz99.cosmos.util;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;

/**
 * A simple util class used to convert double timestamp to int timestamp in map
 */
public class TimestampUtil {

    /**
     * convert _ts field in map from Double to Long for compatibility of cosmosdb and mongodb
     *
     * <p>
     * cosmosdb uses Long _ts
     * mongodb uses Double _ts for sort stability
     * </p>
     *
     * @param map
     */
    public static void processTimestampPrecision(Map<String, Object> map) {

        if (MapUtils.isEmpty(map)) {
            return;
        }

        var ts = map.get("_ts");

        if (ts == null) {
            // do nothing
            return;
        }

        if (ts instanceof Double) {
            //for compatibility, convert 1714546148.123d to 1714546148L
            map.put("_ts", ((Double) ts).longValue());
        }
    }

}
