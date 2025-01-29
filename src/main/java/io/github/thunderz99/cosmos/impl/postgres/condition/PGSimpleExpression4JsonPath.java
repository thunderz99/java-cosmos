package io.github.thunderz99.cosmos.impl.postgres.condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGFilterOptions;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

/**
 * A class representing simple json path expression, which is used in Condition.join query
 * <p>
 * {@code
 *  // simple expression for json path expression
 *  // ==
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no == "001")'::jsonpath)
 *  // >
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no > "001")'::jsonpath)
 *  // LIKE
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no like_regex "^001.*")'::jsonpath)
 * }
 */
public class PGSimpleExpression4JsonPath implements Expression {

	public static final Pattern binaryOperatorPattern = Pattern.compile("^\\s*(IN|=|!=|<|<=|>|>=)\\s*$");

	public String key;
	public Object value;
	public OperatorType type = OperatorType.BINARY_OPERATOR;
    public PGFilterOptions filterOptions = PGFilterOptions.create();

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

    public PGSimpleExpression4JsonPath() {
    }

    public PGSimpleExpression4JsonPath(String key, Object value, PGFilterOptions filterOptions) {
        this.key = key;
        this.value = value;

        // for jsonPath expression, the filterOptions.join must not be empty
        Checker.checkNotNull(filterOptions, "filterOptions");
        Checker.checkNotEmpty(filterOptions.join, "filterOptions");
        this.filterOptions = filterOptions;

    }

    public PGSimpleExpression4JsonPath(String key, Object value, String operator, PGFilterOptions filterOptions) {
        this.key = key;
        this.value = value;
        this.operator = operator;
        this.type = binaryOperatorPattern.asPredicate().test(operator) ? OperatorType.BINARY_OPERATOR
                : OperatorType.BINARY_FUNCTION;

        // for jsonPath expression, the filterOptions.join must not be empty
        Checker.checkNotNull(filterOptions, "filterOptions");
        Checker.checkNotEmpty(filterOptions.join, "filterOptions");
        this.filterOptions = filterOptions;

    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var joinKey = "";
        for(var subKey : this.filterOptions.join) {
            Checker.checkNotBlank(subKey, "key");
            if(StringUtils.contains(this.key, subKey)){
                joinKey = subKey;
            }
        }

        if(StringUtils.isEmpty(joinKey)){
            // joinKey not match, so this is a normal PGSimpleExpression
            var exp = new PGSimpleExpression(this.key, this.value, this.operator);
            return exp.toQuerySpec(paramIndex, selectAlias);
        }

        // key format used in jsonb_path. e.g. ("$.area.city.street.rooms[*]", "@.no")
        var keyPair = PGKeyUtil.getJsonbPathKey(joinKey, key);

        // "$.area.city.street.rooms[*]"
        var jsonbPath = keyPair.getLeft();

        // "@.no"
        var jsonbKey = keyPair.getRight();

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
            // e.g ( parentId IN (@parentId__0, @parentId__1, @parentId__2) )
            var coll = (Collection<?>) paramValue;

            // array equals or not
            if (Set.of("=", "!=").contains(this.operator)) {
                var queryText = " %s @?? %s::jsonpath".formatted(selectAlias, paramName);

                // replace "=" to "==" for json path expression
                var op = StringUtils.equals("=", this.operator) ? "==" : this.operator;

                // $.area.city.street.rooms[*] ? (@.no == "001")
                // in the json path expression, do not need to use ??, just use ? is ok
                var value = " (%s ? (%s %s %s))".formatted(jsonbPath, jsonbKey, op, JsonUtil.toJson(paramValue));
                var param = Condition.createSqlParameter(paramName, value);

                ret.setQueryText(queryText);
                params.add(param);
            } else {
                //the default operator for collection "IN"
                // c.foo IN ['A','B','C']
                if (coll.isEmpty()) {
                    //if paramValue is empty, return a FALSE queryText.
                    ret.setQueryText(" (1=0)");
				} else {
					// use IN array
                    // TODO join buildInArray
                    var queryText = buildInArray(this.key, paramName, coll, params);
					ret.setQueryText(queryText);
				}
			}

		} else {
            // single param value

            if (StringUtils.isEmpty(this.operator) || StringUtils.equals("=", this.operator)) {
                // set the default operator for scalar value
                this.operator = "==";
            }

            // paramName or fieldName
            if (paramValue instanceof FieldKey) {
                throw new NotImplementedException("FieldKey is not supported in join query");
                // valuePart should be "mail2" for "data.mail != data.mail2"
                //valueString = PGKeyUtil.getFormattedKeyWithAlias(((FieldKey) paramValue).keyName, selectAlias, paramValue);
            }

            if (this.type == OperatorType.BINARY_OPERATOR) { // operators, e.g. =, !=, <, >
                //use op for cosmosdb reserved words
                var op = this.operator;

                var queryText = " %s @?? %s::jsonpath".formatted(selectAlias, paramName);

                // $.area.city.street.rooms[*] ? (@.no == "001")
                var valuePart = (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;

                // in the json path expression, do not need to use ??, just use ? is ok
                var value = " (%s ? (%s %s %s))".formatted(jsonbPath, jsonbKey, op, valuePart);
                var param = Condition.createSqlParameter(paramName, value);

                ret.setQueryText(queryText);
                params.add(param);

            } else if (Condition.typeCheckFunctionPattern.asMatchPredicate().test(this.operator)) { // type check funcs: IS_DEFINED|IS_NUMBER|IS_PRIMITIVE, etc
                throw new NotImplementedException("typeCheckFunctions are not supported in join query");
                //ret.setQueryText(String.format(" (%s(%s) = %s)", this.operator, formattedKey, paramName));
            } else { // other binary functions. e.g. STARTSWITH, CONTAINS, ARRAY_CONTAINS
                buildBinaryFunctionDetails(ret, jsonbPath, jsonbKey, paramName, paramValue, params, selectAlias);
            }

		}

		ret.setParameters(params);

		return ret;

	}

    void buildBinaryFunctionDetails(CosmosSqlQuerySpec querySpec, String jsonbPath, String jsonbKey, String paramName, Object paramValue, List<CosmosSqlParameter> params, String selectAlias) {

        var queryText = " %s @?? %s::jsonpath".formatted(selectAlias, paramName);

        switch (this.operator.toUpperCase()) {
            case "STARTSWITH"-> {
                querySpec.setQueryText(queryText);

                // in the json path expression, do not need to use ??, just use ? is ok
                // in json path expression, "STARTSWITH" is "starts with"
                // $.area.city.street.rooms[*] ? (@.no == "001")
                var valuePart = (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;

                var value = " (%s ? (%s starts with %s))".formatted(jsonbPath, jsonbKey, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);

            }
            case "CONTAINS" -> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "CONTAINS"

                var valuePart = ".*\s.*".formatted(paramValue);

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "LIKE"-> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "LIKE"

                var regexValue = paramValue.toString()
                        .replace("%", ".*")  // % to match any number of characters
                        .replace("_", ".");  // _ to match exactly one character

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, regexValue);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "REGEXMATCH" -> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "REGEXMATCH"

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, paramValue);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "ARRAY_CONTAINS" -> {
                // use "@[*] == 2" or "@[*] == \"A\""

                querySpec.setQueryText(queryText);

                var jsonbKey4Array = jsonbKey + "[*]";

                var valuePart =  (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;

                // use ==
                var value = " (%s ? (%s == \"%s\"))".formatted(jsonbPath, jsonbKey, valuePart);

                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);

            }
            default -> {
                querySpec.setQueryText(queryText);

                var valuePart =  (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;
                var value = " (%s ? (%s %s %s))".formatted(jsonbPath, jsonbKey, this.operator, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
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
    static String buildInArray(String key, String paramName, Collection<?> paramValue, List<CosmosSqlParameter> params) {
        var ret = String.format(" (%s = ANY(%s))", PGKeyUtil.getFormattedKey(key), paramName);
        params.add(Condition.createSqlParameter(paramName, paramValue));
        return ret;
    }

}
