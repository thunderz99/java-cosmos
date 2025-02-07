package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.util.FieldNameUtil;
import io.github.thunderz99.cosmos.util.NumberUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;

import static io.github.thunderz99.cosmos.util.AggregateUtil.extractFieldFromFunction;
import static io.github.thunderz99.cosmos.util.AggregateUtil.extractFunctionAndAlias;

/**
 * A util class that contains util method for aggregate
 */
public class PGAggregateUtil {


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
}
