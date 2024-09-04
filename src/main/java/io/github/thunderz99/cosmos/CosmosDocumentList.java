package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.TimestampUtil;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Represent a list of CosmosDB document.
 *
 * <p>
 * Having toList and toJson util method to convert to {@code List<T>} or String
 * conveniently.
 * </p>
 */
public class CosmosDocumentList {

    List<Map<String, Object>> maps;

    public CosmosDocumentList() {
    }

    public CosmosDocumentList(List<?> objs) {

        if (CollectionUtils.isEmpty(objs)) {
            return;
        }

        var obj = objs.get(0);

        if (obj instanceof Map) {
            this.maps = (List<Map<String, Object>>) objs;
            for (var map : maps) {
                TimestampUtil.processTimestampPrecision(map);
            }
        }
    }

    public <T> List<T> toList(Class<T> classOfT) {

        if (maps != null) {
            return maps.stream().map(obj -> JsonUtil.fromMap(obj, classOfT)).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    public List<Map<String, Object>> toMap() {

        if (maps != null) {
            return maps.stream().map(obj -> Maps.newLinkedHashMap(obj)).collect(Collectors.toList());
        }

        return Lists.newArrayList();
    }

    public int size() {
        if (maps != null) {
            return maps.size();
        }
        return 0;
    }

    public String toJson() {
        if (maps != null) {
            return JsonUtil.toJson(maps);
        }

        return "[]";
    }

    public String toString() {
        return toJson();
    }

}
