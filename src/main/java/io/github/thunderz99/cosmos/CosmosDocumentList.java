package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Represent a list of CosmosDB document.
 *
 * <p>
 * Having toList and toJson util method to convert to {@code List<T>} or String
 * conveniently.
 * </p>
 */
public class CosmosDocumentList {

    List<Map<String, Object>> mapObjs = List.of();


    public CosmosDocumentList(List<Map<String, Object>> mapObjs) {
        this.mapObjs = mapObjs;
    }

    public <T> List<T> toList(Class<T> classOfT) {

        if (mapObjs == null) {
            return List.of();
        }

        return mapObjs.stream().map(obj -> JsonUtil.fromMap(obj, classOfT)).collect(Collectors.toList());
    }

    public List<Map<String, Object>> toMap() {
        return mapObjs;

    }

    public int size() {
        return mapObjs.size();
    }

    public String toJson() {
        return JsonUtil.toJson(mapObjs);
    }

    public String toString() {
        return toJson();
    }

}
