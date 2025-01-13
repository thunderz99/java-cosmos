package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class representing simple expression for postgres
 * <p>
 * {@code
 * data->>'id' = "001", (data->>'age')::int > 15,  data->'skills' ? 'java' (means CONTAINS "java"), and other simple filter
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
        this.type = binaryOperatorPattern.asPredicate().test(operator) ? OperatorType.BINARY_OPERATOR
                : OperatorType.BINARY_FUNCTION;
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
                //the default operator for collection
                //ARRAY_CONTAINS
                if (coll.isEmpty()) {
                    //if paramValue is empty, return a FALSE queryText.
                    ret.setQueryText(" (1=0)");
				} else {
					// use ARRAY_CONTAINS by default to minimize the sql length
					ret.setQueryText(buildArrayContains(this.key, paramName, coll, params));
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
                params.add(Condition.createSqlParameter(paramName, paramValue));
            }

            // other types
            var formattedKey = PGKeyUtil.getFormattedKeyWithAlias(this.key, selectAlias, paramValue);
            if (this.type == OperatorType.BINARY_OPERATOR) { // operators, e.g. =, !=, <, >, LIKE
                //use c["key"] for cosmosdb reserved words
                ret.setQueryText(String.format(" (%s %s %s)", formattedKey, this.operator, valueString));
            } else if (Condition.typeCheckFunctionPattern.asMatchPredicate().test(this.operator)) { // type check funcs: IS_DEFINED|IS_NUMBER|IS_PRIMITIVE, etc
                ret.setQueryText(String.format(" (%s(%s) = %s)", this.operator, formattedKey, valueString));
            } else { // other binary functions. e.g. STARTSWITH, CONTAINS, ARRAY_CONTAINS
                buildBinaryFunctionDetails(ret, formattedKey, valueString, paramValue, params);
            }

		}

		ret.setParameters(params);

		return ret;

	}

    void buildBinaryFunctionDetails(CosmosSqlQuerySpec querySpec, String formattedKey, String valuePart, Object paramValue, List<CosmosSqlParameter> params) {

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
                // use contents->'skills' @> '"Java"'
                // modify the formattedKey from ->> to -> , because skills should be a json array to do ARRAY_CONTAINS
                var jsonKey = formattedKey.replace("->>", "->");
                querySpec.setQueryText(String.format(" (%s @> %s::jsonb)", jsonKey, valuePart));

                // modify the paramValue to "\"paramValue\""
                var currentIndex = params.size() - 1;
                var param = params.get(currentIndex);
                param.value = "\"" + param.getValue() + "\"";
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
     * A helper function to generate c.foo IN (...) queryText
     * <p>
     * INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params OUTPUT:
     * "( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )", and add
     * paramsValue into params
     */
    static String buildArray(String key, String paramName, Collection<?> paramValue, List<CosmosSqlParameter> params) {
        var ret = new StringBuilder(String.format(" (%s IN (", Condition.getFormattedKey(key)));

        int index = 0;

        var paramNameList = new ArrayList<String>();
        for (var v : paramValue) {
            var paramNameIdx = String.format("%s__%d", paramName, index);
            paramNameList.add(paramNameIdx);
            params.add(Condition.createSqlParameter(paramNameIdx, v));
            index++;
        }
        ret.append(paramNameList.stream().collect(Collectors.joining(", ")));

        ret.append("))");

        return ret.toString();
    }

    /**
     * A helper function to generate c.foo ARRAY_CONTAINS (...) queryText
     * <p>
     * INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params OUTPUT:
     * "(ARRAY_CONTAINS("@parentId", c["parentId"]))", and add
     * paramsValue into params
     */
    static String buildArrayContains(String key, String paramName, Collection<?> paramValue, List<CosmosSqlParameter> params) {

        var ret = String.format(" (%s = ANY(%s))", PGKeyUtil.getFormattedKey(key), paramName);

        params.add(Condition.createSqlParameter(paramName, paramValue));
        return ret;
    }

}
