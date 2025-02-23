package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * A class representing simple expression for postgres
 * <p>
 * {@code
 * data->>'id' = "001", (data->>'age')::numeric > 15,  data->'skills' @> 'java' (means CONTAINS "java"), and other simple filter
 * }
 */
public class PGSimpleExpression implements Expression {

	public static final Pattern binaryOperatorPattern = Pattern.compile("^\\s*(LIKE|IN|=|!=|<|<=|>|>=)\\s*$");

	public String key;
	public Object value;
	public OperatorType type = OperatorType.BINARY_OPERATOR;

	/**
	 * Default is empty, which means the default operator based on filter's key and value
	 *
	 *     The difference between "=" and the default operator "":
	 *     <div>e.g.</div>
	 *     <ul>
	 *     	 <li>{@code {"status": ["A", "B"]} means status is either A or B } </li>
	 *       <li>{@code {"status =": ["A", "B"]} means status equals ["A", "B"] } </li>
	 *     </ul>
	 */
    public String operator = "";


    /**
     * maps of type check functions from cosmosdb to postgres
     */
    public static final Map<String, String> TYPE_CHECK_FUNCTIONS_MAP = Map.of(
            "IS_NULL", "null",
            "IS_NUMBER", "number",
            "IS_STRING", "string",
            "IS_BOOL", "boolean",
            "IS_ARRAY", "array",
            "IS_OBJECT", "object"
    );

    public PGSimpleExpression() {
    }

    public PGSimpleExpression(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public PGSimpleExpression(String key, Object value, String operator) {
        this.key = key;
        this.value = value;
        this.operator = operator;
        if(!StringUtils.isEmpty(operator)){
            this.type = binaryOperatorPattern.asPredicate().test(operator) ? OperatorType.BINARY_OPERATOR
                    : OperatorType.BINARY_FUNCTION;
        }
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var ret = new CosmosSqlQuerySpec();
        var params = new ArrayList<CosmosSqlParameter>();

        // fullName.last -> @param001_fullName__last
        // or
        // "829cc727-2d49-4d60-8f91-b30f50560af7.name" -> @param001_wg31gsa.name
        var paramName = ParamUtil.getParamNameFromKey(this.key, paramIndex.getAndIncrement());

        var paramValue = this.value;

        // Robust process for operator IN and paramValue is not Collection

        if ("IN".equals(this.operator) && !(paramValue instanceof Collection<?>)) {
            paramValue = List.of(paramValue);
        }

        if (paramValue instanceof Collection<?>) {
            // collection param value
            // e.g ( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )
            var coll = (Collection<?>) paramValue;

            // array equals or not
            if (Set.of("=", "!=").contains(this.operator)) {
                // use = or !=
                ret.setQueryText(String.format(" (%s %s %s)", PGKeyUtil.getFormattedKeyWithAlias(this.key, selectAlias, paramValue), this.operator, paramName));
                params.add(Condition.createSqlParameter(paramName, paramValue));
            } else {
                //the default operator for collection "IN"
                // c.foo IN ['A','B','C']
                if (coll.isEmpty()) {
                    //if paramValue is empty, return a FALSE queryText.
                    ret.setQueryText(" (1=0)");
				} else {
					// use IN array
					ret.setQueryText(buildInArray(this.key, paramName, coll, params, selectAlias));
				}
			}

		} else {
            // single param value

            if (StringUtils.isEmpty(this.operator)) {
                // set the default operator for scalar value
                this.operator = "=";
            }

            // paramName or fieldName
            var valueString = "";

            if (paramValue instanceof FieldKey) {
                // valuePart should be "mail2" for "data.mail != data.mail2"
                valueString = PGKeyUtil.getFormattedKeyWithAlias(((FieldKey) paramValue).keyName, selectAlias, paramValue);
            } else {
                // valuePart should be "@param001_wg31gsa"
                valueString = paramName;
            }

            // other types
            var formattedKey = PGKeyUtil.getFormattedKeyWithAlias(this.key, selectAlias, paramValue);
            if (this.type == OperatorType.BINARY_OPERATOR) { // operators, e.g. =, !=, <, >, LIKE
                //use c["key"] for cosmosdb reserved words
                ret.setQueryText(String.format(" (%s %s %s)", formattedKey, this.operator, valueString));
                params.add(Condition.createSqlParameter(paramName, paramValue));

            } else if (Condition.typeCheckFunctionPattern.asMatchPredicate().test(this.operator)) { // type check funcs: IS_DEFINED|IS_NUMBER|IS_PRIMITIVE, etc

                // params not needed to be added
                // because true/false converts to json_path_exists or NOT json_path_exists
                buildTypeCheckFunctionDetails(ret, selectAlias, this.key, this.operator, paramValue, params);


            } else { // other binary functions. e.g. STARTSWITH, CONTAINS, ARRAY_CONTAINS
                params.add(Condition.createSqlParameter(paramName, paramValue));
                buildBinaryFunctionDetails(ret, formattedKey, valueString, paramValue, params);
            }

		}

		ret.setParameters(params);

		return ret;

	}

    /**
     * Build query text for typeCheckFunctions. e.g. IS_DEFINED, IS_NUMBER, IS_PRIMITIVE, etc.
     * see docs/postgres-type-check-functions.md for details
     *
     * @param querySpec the query specification to build
     * @param key the key as column name, e.g. address.city.street
     * @param selectAlias typically "data" (or "j1" "s1" for cond.join query) (or empty for afterAggregation)
     * @param paramValue the original paramValue, e.g. true
     * @param params the list of params to add paramValue
     */
    static void buildTypeCheckFunctionDetails(CosmosSqlQuerySpec querySpec, String selectAlias, String key, String op, Object paramValue, List<CosmosSqlParameter> params) {

        Checker.checkNotNull(querySpec, "querySpec");
        Checker.checkNotBlank(selectAlias,
                "For typeCheckFunctions, selectAlias cannot be empty. Which means typeCheckFunctions in condAfterAggregation is not supported");
        Checker.checkNotBlank(key, "key");
        Checker.check(paramValue instanceof Boolean, "paramValue must be boolean for typeCheckFunctions");
        Checker.checkNotNull(params, "params");

        var positiveTypeCheck = (Boolean) paramValue;  // e.g. Condition.filter("x IS_NULL", true/false)

        // Convert the nested key (like "address.city.street") into a JSONPath expression "$.address.city.street"
        var jsonPathKey =  PGKeyUtil.getJsonbPathKey(key);

        String pathExpression;                          // will become something like '$.address.city.street ? (@.type() == "number")'

        // 1) Check if it's one of the simple type-based ops in the TYPE_CHECK_FUNCTIONS_MAP
        String typeStr = TYPE_CHECK_FUNCTIONS_MAP.getOrDefault(op, "");

        if (!typeStr.isEmpty()) {
            // e.g. op = "IS_NUMBER" => typeStr = "number"
            // Build a JSONPath expression that checks the type:
            // '$.address.city.street ? (@.type() == "number")'
            pathExpression = String.format(
                    "%s ? (@.type() == \"%s\")",
                    jsonPathKey,
                    typeStr
            );
        } else if ("IS_DEFINED".equals(op)) {
            // Check just that the path is present, ignoring its type:
            // '$.address.city.street'
            pathExpression = String.format(
                    "%s",
                    jsonPathKey
            );
        } else if ("IS_PRIMITIVE".equals(op)) {
            // "primitive" => null, boolean, number, or string
            //  => '$.address.city.street ? (@.type() == "null" || @.type() == "boolean" || @.type() == "number" || @.type() == "string")'
            pathExpression = String.format(
                    "%s ? (@.type() == \"null\" || @.type() == \"boolean\" || @.type() == \"number\" || @.type() == \"string\")",
                    jsonPathKey
            );
        } else {
            // If you somehow get here with an unknown operator, either throw or build a default expression
            throw new IllegalArgumentException("Unknown typeCheckFunction: " + op);
        }

        // 2) Combine into a jsonb_path_exists(...) call.
        // The final expression should compare = true or = false based on paramValue.
        // e.g. "(jsonb_path_exists(data, '$.x ? (@.type() == "number")') = true)"
        String queryText = String.format(
                " jsonb_path_exists(%s, '%s')",
                selectAlias,
                pathExpression
        );

        if (!positiveTypeCheck) {
            queryText = " (NOT %s)".formatted(queryText);
        }
        querySpec.setQueryText(queryText);
    }

    /**
     * Build query text for binaryFunctions, e.g. STARTSWITH, CONTAINS, REGEXMATCH, ARRAY_CONTAINS, etc
     *
     * @param querySpec
     * @param formattedKey the formatted key as column name, e.g. data->>'name'
     * @param valuePart the part of paramValue, e.g. true
     * @param paramValue the original paramValue, e.g. true
     * @param params the list of params to add paramValue
     */
    void buildBinaryFunctionDetails(CosmosSqlQuerySpec querySpec, String formattedKey, String valuePart, Object paramValue, List<CosmosSqlParameter> params) {

        /**
         * STARTSWITH, CONTAINS, REGEXMATCH, ARRAY_CONTAINS
         *
         * STARTSWITH: use LIKE
         * CONTAINS: use LIKE
         * REGEXMATCH: use `text_field ~ '^abc'`
         * ARRAY_CONTAINS: use data->'skills' ?? 'Java'
         */
        switch (this.operator.toUpperCase()) {
            case "STARTSWITH"-> {
                // use LIKE
                querySpec.setQueryText(String.format(" (%s LIKE %s)", formattedKey, valuePart));

                // modify the paramValue to "%paramValue%"
                var currentIndex = params.size() - 1;
                var param = params.get(currentIndex);
                param.value = param.getValue() + "%";
            }
            case "CONTAINS" -> {
                // use LIKE
                querySpec.setQueryText(String.format(" (%s LIKE %s)", formattedKey, valuePart));

                // modify the paramValue to "%paramValue%"
                var currentIndex = params.size() - 1;
                var param = params.get(currentIndex);
                param.value = "%" + param.getValue() + "%";
            }
            case "REGEXMATCH" -> {
                // use `text_field ~ '^abc'`
                querySpec.setQueryText(String.format(" (%s ~ %s)", formattedKey, valuePart));
            }
            case "ARRAY_CONTAINS" -> {
                // use data->'skills' ?? 'Java'
                // because ? is also a placeholder for PreparedStatement, ?? insteadof ? in JDBC
                // https://stackoverflow.com/questions/26516204/how-do-i-escape-a-literal-question-mark-in-a-jdbc-prepared-statement
                // modify the formattedKey from ->> to -> , because skills should be a json array to do ARRAY_CONTAINS
                var jsonKey = formattedKey.replace("->>", "->");
                if(paramValue instanceof Integer) {
                    // you can not use ?? for integer. instead, use @>

                    // modify the paramValue to String type paramValue
                    var currentIndex = params.size() - 1;
                    var param = params.get(currentIndex);
                    param.value = String.valueOf(param.getValue());

                    // modify the key to data->'no' (without ::int to do a jsonb contains)
                    jsonKey = StringUtils.removeStart(jsonKey,"(");
                    jsonKey = StringUtils.removeEnd(jsonKey,")::numeric");

                    querySpec.setQueryText(String.format(" (%s @> %s::jsonb)", jsonKey, valuePart));
                } else {
                    querySpec.setQueryText(String.format(" (%s ?? %s)", jsonKey, valuePart));
                }
            }
            default -> {
                querySpec.setQueryText(String.format(" (%s(%s, %s))", this.operator, formattedKey, valuePart));
            }
        }


    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }


    /**
     * A helper function to generate c.foo IN @param000_array1 queryText
     * <p>
     * INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params OUTPUT:
     * (data->>'parentId' = ANY(@parentId))
     * paramsValue into params
     * </p>
     */
    static String buildInArray(String key, String paramName, Collection<?> paramValue, List<CosmosSqlParameter> params, String selectAlias) {
        var ret = String.format(" (%s = ANY(%s))", PGKeyUtil.getFormattedKeyWithAlias(key, selectAlias, ""), paramName);
        params.add(Condition.createSqlParameter(paramName, paramValue));
        return ret;
    }

}
