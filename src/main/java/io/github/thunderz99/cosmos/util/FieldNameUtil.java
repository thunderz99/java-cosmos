package io.github.thunderz99.cosmos.util;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * A util class convert json field names
 */
public class FieldNameUtil {

    /**
     * Convert "c['address']['city']['street']" to "address.city.street"
     *
     * <p>
     * And also "c.address.city.street" to "address.city.street"
     * </p>
     *
     * @param input
     * @return fieldName using dot
     */
    public static String convertToDotFieldName(String input) {
        if (input == null) {
            return null;
        }

        // Remove the c. prefix used in cosmosdb
        input = StringUtils.removeStart(input, "c.");

        // Regex to match the pattern c['key1']['key2'] or c["key1"]["key2"]...
        var pattern = Pattern.compile("\\[['\"]([^'\"]+)['\"]\\]");
        var matcher = pattern.matcher(input);

        var result = new StringBuilder();

        while (matcher.find()) {
            if (result.length() > 0) {
                result.append(".");
            }
            result.append(matcher.group(1));
        }

        // If the result is empty and the input is a single key without brackets
        if (result.length() == 0 && !StringUtils.containsAny(input, "['", "[\"")) {
            return input;
        }

        return result.toString();
    }
}
