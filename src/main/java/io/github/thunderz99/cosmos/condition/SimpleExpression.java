package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.Collection;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;

import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * A class representing simple expression
 *
 * {@code
 * c.id = "001", c.age > 15, c.skills CONTAINS "java", and other simple filter
 * }
 */
public class SimpleExpression implements Expression {

	public static final Pattern binaryOperatorPattern = Pattern.compile("^\\s*(LIKE|IN|=|!=|<|<=|>|>=)\\s*$");

	public String key;
	public Object value;
	public OperatorType type = OperatorType.BINARY_OPERATOR;
	public String operator = "=";

	public SimpleExpression() {
	}

	public SimpleExpression(String key, Object value) {
		this.key = key;
		this.value = value;
	}

	public SimpleExpression(String key, Object value, String operator) {
		this.key = key;
		this.value = value;
		this.operator = operator;
		this.type = binaryOperatorPattern.asPredicate().test(operator) ? OperatorType.BINARY_OPERATOR
				: OperatorType.BINARY_FUNCTION;
	}

	@Override
	public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex) {

		var ret = new SqlQuerySpec();
		var params = new SqlParameterCollection();

		// fullName.last -> @param001_fullName__last
		var paramName = String.format("@param%03d_%s", paramIndex.get(), this.key.replace(".", "__"));
		var paramValue = this.value;

		if (paramValue instanceof Collection<?>) {
			// e.g ( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )
			if (!"=".equals(this.operator) && !"IN".equals(this.operator)) {
				throw new IllegalArgumentException("IN collection expression not supported for " + this.operator);
			}

			var coll = (Collection<?>) paramValue;

			if(coll.isEmpty()){
				//if paramValue is empty, return a FALSE queryText.
				ret.setQueryText(" (1=0)");
			} else if("IN".equals(this.operator)){
				// use IN
				paramIndex.getAndIncrement();
				ret.setQueryText(buildArray(this.key, paramName, (Collection<?>) paramValue, params));
			} else {
				// use ARRAY_CONTAINS by default to minimize the sql length
				paramIndex.getAndIncrement();
				ret.setQueryText(buildArrayContains(this.key, paramName, (Collection<?>) paramValue, params));
			}

		} else {

			paramIndex.getAndIncrement();
			// other types
			var formattedKey = Condition.getFormattedKey(this.key);
			if (this.type == OperatorType.BINARY_OPERATOR) {
				//use c["key"] for cosmosdb reserved words
				ret.setQueryText(String.format(" (%s %s %s)", formattedKey, this.operator, paramName));
			} else {

				ret.setQueryText(String.format(" (%s(%s, %s))", this.operator, formattedKey, paramName));
			}

			params.add(Condition.createSqlParameter(paramName, paramValue));
		}

		ret.setParameters(params);

		return ret;

	}

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}

	/**
	 * A helper function to generate c.foo IN (...) queryText
	 *
	 * INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params OUTPUT:
	 * "( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )", and add
	 * paramsValue into params
	 */
	static String buildArray(String key, String paramName, Collection<?> paramValue, SqlParameterCollection params) {
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
	 *
	 * INPUT: "parentId", "@parentId", ["id001", "id002", "id005"], params OUTPUT:
	 * "(ARRAY_CONTAINS("@parentId", c["parentId"]))", and add
	 * paramsValue into params
	 */
	static String buildArrayContains(String key, String paramName, Collection<?> paramValue, SqlParameterCollection params) {
		var ret = String.format(" (ARRAY_CONTAINS(%s, %s))", paramName, Condition.getFormattedKey(key));
		params.add(Condition.createSqlParameter(paramName, paramValue));
		return ret;
	}

}
