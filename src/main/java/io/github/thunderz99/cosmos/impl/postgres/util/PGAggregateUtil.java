package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.NumberUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.postgresql.util.PGobject;

/**
 * A util class that contains util method for aggregate
 */
public class PGAggregateUtil {


    /**
     * objectMapper used to convert PGobject to java object
     */
    static ObjectMapper objectMapper = JsonUtil.getObjectMapper();;

    /**
     * Process result of aggregate. convert Long value to Integer if possible.
     * <p>
     * Because "itemsCount: 1L" is not acceptable by some users. They prefer "itemsCount: 1" more.
     * </p>
     *
     * @param maps
     * @return
     */
    public static List<Map<String, Object>> convertAggregateResultsToInteger(List<Map<String, Object>> maps) {

        if (CollectionUtils.isEmpty(maps)) {
            return maps;
        }

        for (var map : maps) {
            map.replaceAll((key, value) -> {

                // for compatibility with cosmosdb, we need to convert null to empty map
                if(value == null){
                    return new LinkedHashMap<>();
                }

                // check if the value is a String that can be parsed to Long or Integer
                if(value instanceof String strValue && !StringUtils.contains(strValue, ".")){
                    if(NumberUtils.isCreatable(strValue)){
                        value = NumberUtils.createNumber(strValue);
                    }
                }

                // Check if the value is an instance of Long
                if (value instanceof Number) {
                    var numberValue = (Number) value;
                    return NumberUtil.convertNumberToIntIfCompatible(numberValue);
                }
                return value; // Return the original value if no conversion is needed
            });
        }

        return maps;
    }

    /**
     * get the final field name for group like "fullName.last -> last"
     * @param field
     * @return simple field name without "."
     */
    public static String getSimpleName(String field) {
        if (StringUtils.isEmpty(field)) {
            return field;
        }
        return field.contains(".") ? field.substring(field.lastIndexOf(".") + 1) : field;
    }

    /**
     * When doing a "group by" operation on a jsonb field, whose type is an array / a json object, the result contains PGobject type.
     * we should convert it to java object (String/Integer/List/Map)
     *
     * @param maps
     * @return
     */
    public static List<Map<String, Object>> convertPGObjectsToJavaObject(List<Map<String, Object>> maps) {
        if (CollectionUtils.isEmpty(maps)) {
            return maps;
        }

        for (var map : maps) {
            map.replaceAll((key, value) -> {

                // check if the value is a PGobject and convert it to java object, using JsonUtil
                if (value instanceof PGobject obj) {
                    var json = obj.getValue();
                    try {
                        return objectMapper.readValue(json, Object.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("cannot convert PGobject to java object.key = " + key + ", json = " + json, e);
                    }

                }

                // Return the original value if no conversion is needed
                return value;
            });
        }
        return maps;
    }
}
