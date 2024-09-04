package io.github.thunderz99.cosmos.util;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;

/**
 * A simple util class used to convert double timestamp to int timestamp in map
 */
public class TimestampUtil {

    public static void processTimestampPrecision(Map<String, Object> map){

        if(MapUtils.isEmpty(map)){
            return;
        }

        var ts = map.get("_ts");

        if(ts == null){
            // do nothing
            return;
        }

        if(ts instanceof Double){
            //for compatibility, convert 1502246148.123d to 1502246148
            map.put("_ts", ((Double) ts).intValue());
        }
    }

}
