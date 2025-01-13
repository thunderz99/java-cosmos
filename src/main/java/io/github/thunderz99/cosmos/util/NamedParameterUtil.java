package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for converting SQL statements with named parameters into ones with positional parameters and binding the values.
 * <p>
 * This class is used to convert the SQL statement with named parameters into one with positional parameters and bind the values.
 * The converted SQL is modified to replace each named parameter with a positional parameter, e.g., "?". The parameter values are
 * stored in a list and returned as part of a ParsedSql object.
 * </p>
 */
public class NamedParameterUtil {

    /**
     * Converts an SQL statement with named parameters into one with positional parameters and binds the values.
     *
     * @param querySpec the SQL with named parameters, e.g., "SELECT * FROM table WHERE id = @id AND name = @name". And a list of parameter names and their values
     * @return a ParsedSql object containing the converted SQL and the list of parameter values
     */
    public static CosmosSqlQuerySpec convert(CosmosSqlQuerySpec querySpec) {

        var sql = querySpec.queryText;
        var paramMap = querySpec.params.stream().collect(Collectors.toMap(CosmosSqlParameter::getName, CosmosSqlParameter::getValue));

        return convert(sql, paramMap);
    }

    /**
     * Converts an SQL statement with named parameters into one with positional parameters and binds the values.
     *
     * @param sql the SQL with named parameters, e.g., "SELECT * FROM table WHERE id = @id AND name = @name"
     * @param paramMap a map of parameter names and their values
     * @return a ParsedSql object containing the converted SQL and the list of parameter values
     */
    public static CosmosSqlQuerySpec convert(String sql, Map<String, Object> paramMap){

        Checker.checkNotNull(sql, "sql");
        Checker.checkNotNull(paramMap, "paramMap");

        // Regex to match named parameters (e.g., @paramName)
        var namedParamPattern = Pattern.compile("(@[a-zA-Z_][a-zA-Z0-9_]*)");
        var matcher = namedParamPattern.matcher(sql);

        var parsedSql = new StringBuilder();
        var params = new ArrayList<CosmosSqlParameter>();
        var lastIndex = 0;

        while (matcher.find()) {
            // Append SQL text before the current named parameter
            parsedSql.append(sql, lastIndex, matcher.start());

            var paramName = matcher.group(1); // Extract the parameter name
            if (!paramMap.containsKey(paramName)) {
                throw new IllegalArgumentException("Parameter '" + paramName + "' not found in the parameter map");
            }

            params.add(Condition.createSqlParameter(paramName, paramMap.get(paramName))); // Add the parameter value to the list
            parsedSql.append("?");              // Replace the named parameter with a positional one

            lastIndex = matcher.end();
        }

        // Append the remaining SQL text
        parsedSql.append(sql.substring(lastIndex));

        return new CosmosSqlQuerySpec(parsedSql.toString(), params);
    }

}
