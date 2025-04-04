package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import static io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil.*;

/**
 * A util class for postgres sort operations. mainly for "data" column which is JSONB type
 */
public class PGSortUtil {

    /**
     * collate setting representing ANSI C
     * <p>
     *     see docs/postgres-sort-order.md for details
     * </p>
     */
    public static String COLLATE_C = "C";

    /**
     * collate setting representing en_US
     * <p>
     *     see docs/postgres-sort-order.md for details
     * </p>
     */
    public static String COLLATE_EN_US = "en_US";

    /**
     * Sort keys which is predefined with formatted value
     */
    static Set<String> preservedSorts = Set.of("_ts");

    /**
     * Sort types that supported, other types should be sorted as jsonb
     */
    static Set<String> sortTypes = Set.of("int", "float8", "numeric", "text");

    /**
     * generate a formatted key for sort
     * @param key key in dot format address.city.street::text / content.age.value::numeric
     * @param collate "C" or "en_US" or null/"". default to "en_US"
     * @return data->'address'->'city'->>'street' / (data->'content'->'age'->>'value')::numeric
     */
    public static String getFormattedKey4Sort(String key, String sortDirection, String collate) {

        if(StringUtils.isEmpty(collate)){
            collate = COLLATE_EN_US;
        }
        if(!StringUtils.equalsAny(collate, COLLATE_C, COLLATE_EN_US)){
            throw new IllegalArgumentException("collate should be \"C\" or \"en_US\". actual:" + collate);
        }

        if(preservedSorts.contains(key)){
            return "%s %s".formatted(getFormattedKeyWithAlias(key, TableUtil.DATA, ""), sortDirection);
        }

        var collateStr = COLLATE_C.equals(collate) ? " COLLATE \"C\" " : " ";

        if("id".equals(key)){
            return "%s%s%s".formatted("id", collateStr, sortDirection);
        }

        if(PGKeyUtil.textKeys.contains(key)){
            // sort by string, using COLLATE "C" to deal with lower/upper case correctly
            return "%s%s%s".formatted(getFormattedKeyWithAlias(key, TableUtil.DATA, ""), collateStr, sortDirection);
        }

        var parts = key.split("::");

        if(parts.length == 2) {
            // type is defined explicitly
            key = parts[0];
            var type = parts[1];

            if(sortTypes.contains(type)){
                return "text".equals(type) ?
                        "%s%s%s".formatted(getFormattedKey(key), collateStr, sortDirection)
                        : "(%s)::%s %s".formatted(getFormattedKey(key), type, sortDirection);
            }
        }

        // default to jsonb type
        // we will sort it in jsonb type
        // null < booleans < numbers < strings < arrays < objects
        // this will fulfill most needs
        // for details, see docs/postgres-sort-order.md

        var jsonKey = getFormattedKey4JsonWithAlias(key, TableUtil.DATA);

        if(COLLATE_C.equals(collate)) {
            // sort using COLLATE "C"
            var complicatedSortKey = """
                    
                      CASE jsonb_typeof(%s)
                        WHEN 'null' THEN 0
                        WHEN 'boolean' THEN 1
                        WHEN 'number' THEN 2
                        WHEN 'string' THEN 3
                        WHEN 'array' THEN 4
                        WHEN 'object' THEN 5
                        ELSE 6
                      END %s,
                      CASE
                        WHEN jsonb_typeof(%s) = 'string'
                          THEN %s COLLATE "C"
                        ELSE NULL
                      END %s,
                      %s %s
                    """;
            var textKey = getFormattedKey(key);

            return complicatedSortKey.formatted(jsonKey, sortDirection,
                    jsonKey, textKey, sortDirection,
                    jsonKey, sortDirection);

        }


        return " %s %s".formatted(jsonKey, sortDirection);

    }

}
