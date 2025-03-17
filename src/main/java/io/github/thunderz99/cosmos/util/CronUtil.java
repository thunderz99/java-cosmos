package io.github.thunderz99.cosmos.util;

import java.time.Instant;
import java.util.Map;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.apache.commons.collections4.MapUtils;

/**
 * A simple util class used to check a cron expression is valid or not
 */
public class CronUtil {


    /**
     * Check if a cron expression is valid or not.
     *
     * @param expression a cron expression.
     * @return true if the expression is valid, false otherwise.
     *
     * @param expression
     * @return
     */
    public static boolean isValidPgCronExpression(String expression) {
        try {
            // pg_cron uses a 5-field Unix cron format.
            var cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
            var parser = new CronParser(cronDefinition);
            // Parse the expression
            var cron = parser.parse(expression);
            // Validate its syntax (throws an exception if invalid)
            cron.validate();
            return true;
        } catch (Exception e) {
            // If any error occurs during parsing or validation, the expression is invalid.
            return false;
        }
    }

}
