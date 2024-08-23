package io.github.thunderz99.cosmos.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

/**
 * util class that help to generate query bson involving JOIN for mongodb
 */
public class JoinUtil {

    /**
     * Split the condition's filter to 2 types:
     * LEFT: filter that has no relationship to JOIN, which can be executed at the very beginning
     * RIGHT: filter whose field is part of JOIN, which must be executed in the $project's $filter parts
     * @param filter e.g. Map.of("lastName !=", "Andersen", "area.city.street.rooms.no =", "001", "tags.id", 10)
     * @param join e.g. Set.of("area.city.street.rooms", "tags")
     * @return pair of filters
     */
    public static Pair<Map<String, Object>, Map<String, Object>> splitFilters(Map<String, Object> filter, Set<String> join) {
        var leftFilters = new HashMap<String, Object>();
        var rightFilters = new HashMap<String, Object>();

        // Iterate over each entry in the filter map
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            // Determine if the field is related to a join field
            boolean isJoinRelated = join.stream().anyMatch(field::startsWith);

            if (isJoinRelated) {
                rightFilters.put(field, value);
            } else {
                leftFilters.put(field, value);
            }
        }

        return Pair.of(leftFilters, rightFilters);
    }
}
