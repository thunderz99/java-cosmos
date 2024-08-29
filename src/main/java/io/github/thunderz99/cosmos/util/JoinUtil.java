package io.github.thunderz99.cosmos.util;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.github.thunderz99.cosmos.condition.Condition;

/**
 * util class that help to generate query bson involving JOIN for mongodb
 */
public class JoinUtil {

    /**
     * Extract the filters used in join, so that we can get the only matching elements in sub array.
     *
     * @param filter e.g. Map.of("lastName !=", "Andersen", "area.city.street.rooms.no =", "001", "tags.id", 10)
     * @param join   e.g. Set.of("area.city.street.rooms", "tags")
     * @return filters that is related to join TODO add unit test
     */
    public static Map<String, Object> extractJoinFilters(Map<String, Object> filter, Set<String> join) {

        var joinRelatedFilters = new LinkedHashMap<String, Object>();

        if (filter == null || join == null) {
            return joinRelatedFilters;
        }

        // Iterate over each entry in the filter map
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            // Determine if the field is related to a join field
            boolean isJoinRelated = join.stream().anyMatch(field::startsWith);

            if (isJoinRelated) {
                joinRelatedFilters.put(field, value);
            } else {
                // for nested queries
                if (value instanceof Condition) {
                    joinRelatedFilters.putAll(extractJoinFilters(((Condition) value).filter, join));
                } else if (value instanceof Collection<?>) {
                    for (var subFilter : (Collection<?>) value) {
                        joinRelatedFilters.putAll(extractJoinFilters((Collection<?>) value, join));
                    }
                } else if (value instanceof Map<?, ?>) {
                    joinRelatedFilters.putAll(extractJoinFilters((Map<String, Object>) value, join));
                }
            }
        }
        return joinRelatedFilters;
    }

    /**
     * extract join related filter from a collection of sub filters
     *
     * @param subFilters
     * @param join
     * @return join related filter
     */
    static Map<String, Object> extractJoinFilters(Collection<?> subFilters, Set<String> join) {

        var ret = new LinkedHashMap<String, Object>();

        for (var filter : subFilters) {
            if (filter instanceof Condition) {
                ret.putAll(extractJoinFilters(((Condition) filter).filter, join));
            } else if (filter instanceof Collection<?>) {
                ret.putAll(extractJoinFilters((Collection<?>) filter, join));
            } else if (filter instanceof Map<?, ?>) {
                ret.putAll(extractJoinFilters((Map<String, Object>) filter, join));
            }
        }

        return ret;
    }
}
