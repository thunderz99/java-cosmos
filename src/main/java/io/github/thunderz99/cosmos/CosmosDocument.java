package io.github.thunderz99.cosmos;

import java.util.Map;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Represent a CosmosDB document. Has a JSONObject inside.
 * <p>
 * Having toObject and toJson util method to convert to Class or String
 * conveniently.
 */
public class CosmosDocument {

    /**
     * used for sdk v4
     */
    Map<String, Object> mapObj;

    public CosmosDocument(Map<String, Object> mapObj) {
        this.mapObj = mapObj;
    }
    
    public <T> T toObject(Class<T> classOfT) {
        return mapObj == null ? null
                : JsonUtil.fromMap(mapObj, classOfT);
    }

    public String toJson() {
        return mapObj == null ? null
                : JsonUtil.toJson(mapObj);
    }

    public Map<String, Object> toMap() {
        return mapObj == null ? null
                : mapObj;
    }
}