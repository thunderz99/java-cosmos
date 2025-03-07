package io.github.thunderz99.cosmos.util;

import java.math.BigDecimal;

/**
 * A util for number processing. e.g. convert Long value to compatible Integer value
 */
public class NumberUtil {

    /**
     * Convert Long / Integer / Double / Float to Integer if compatible. If not compatible, remains the origin formatl
     *
     * @param number number to convert
     * @return Integer if compatible, or the origin number
     */
    public static Number convertNumberToIntIfCompatible(Number number) {

        if (number instanceof Integer) {
            return number; // Already an Integer, no conversion needed.
        } else if (number instanceof Long longValue) {
            if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                return longValue.intValue(); // Convert to Integer if within range.
            }
        } else if (number instanceof Double doubleValue)  {
            if (doubleValue >= Integer.MIN_VALUE && doubleValue <= Integer.MAX_VALUE && doubleValue % 1 == 0) {
                return doubleValue.intValue(); // Convert to Integer if within range and has no fractional part.
            }
        } else if (number instanceof Float floatValue) {
            if (floatValue >= Integer.MIN_VALUE && floatValue <= Integer.MAX_VALUE && floatValue % 1 == 0) {
                return floatValue.intValue(); // Convert to Integer if within range and has no fractional part.
            }
        } else if (number instanceof BigDecimal bigDecimalValue) {
            if(bigDecimalValue.stripTrailingZeros().scale() <= 0
                    && bigDecimalValue.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0
                    && bigDecimalValue.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) <= 0) {
                return bigDecimalValue.intValue();
            }
        }
        // Return the original number if it cannot be converted to Integer.
        return number;
    }

}
