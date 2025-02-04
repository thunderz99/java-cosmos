package io.github.thunderz99.cosmos.util;

import java.time.Instant;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

/**
 * A simple util class used to convert double timestamp to int timestamp in map
 */
public class TimestampUtil {


    /**
     * Get epoch seconds with millis as double(e.g. 1714546148.123d)
     * <p>
     * so when we use sort on _ts, we can get a more stable sort order
     * </p>
     *
     * @return timestamp in double
     */
    public static Double getTimestampInDouble() {
        return Instant.now().toEpochMilli() / 1000d;
    }

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

        if (ts instanceof Double doubleTs) {
            //for compatibility, convert 1714546148.123d to 1714546148L
            map.put("_ts", doubleTs.longValue());
        } else if( ts instanceof Integer intTs){
            map.put("_ts", intTs.longValue());
        }
    }

}
