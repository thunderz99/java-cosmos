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

public class SimpleExpression implements Expression {

	public static final Pattern functionPattern = Pattern.compile("\\w+");

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
		this.type = functionPattern.asPredicate().test(operator) ? OperatorType.BINARY_FUNCTION
				: OperatorType.BINARY_OPERATOR;
	}

	@Override
	public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex) {

		var ret = new SqlQuerySpec();
		var params = new SqlParameterCollection();

		// fullName.last -> @param001_fullName__last
		var paramName = String.format("@param%03d_%s", paramIndex.getAndIncrement(), this.key.replace(".", "__"));
		var paramValue = this.value;

		if (paramValue instanceof Collection<?>) {
			// e.g ( c.parentId IN (@parentId__0, @parentId__1, @parentId__2) )
			if (!"=".equals(this.operator) && !"IN".equals(this.operator)) {
				throw new IllegalArgumentException("IN collection expression not supported for " + this.operator);
			}
			ret.setQueryText(buildArray(this.key, paramName, (Collection<?>) paramValue, params));

		} else {

			// other types
			if (this.type == OperatorType.BINARY_OPERATOR) {
				ret.setQueryText(String.format(" (c.%s %s %s)", this.key, this.operator, paramName));
			} else {
				ret.setQueryText(String.format(" (%s(c.%s, %s))", this.operator, this.key, paramName));
			}
			params.add(new SqlParameter(paramName, paramValue));
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
		var ret = new StringBuilder(String.format(" (c.%s IN (", key));

		int index = 0;

		var paramNameList = new ArrayList<String>();
		for (var v : paramValue) {
			var paramNameIdx = String.format("%s__%d", paramName, index);
			paramNameList.add(paramNameIdx);
			params.add(new SqlParameter(paramNameIdx, v));
			index++;
		}
		ret.append(paramNameList.stream().collect(Collectors.joining(", ")));

		ret.append("))");

		return ret.toString();
	}
}
