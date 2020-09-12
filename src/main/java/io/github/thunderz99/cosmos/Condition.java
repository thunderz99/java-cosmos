package io.github.thunderz99.cosmos;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;

import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;

public class Condition {

	private static Logger log = LoggerFactory.getLogger(Condition.class);

	public Condition() {
	}

	public Map<String, Object> filter = new LinkedHashMap<>();

	public List<String> sort = List.of("_ts", "DESC");

	public Set<String> fields = new LinkedHashSet<>();

	public int offset = 0;
	public int limit = 100;

	public static Condition filter(Object... filters) {

		Condition cond = new Condition();
		if (filters == null || filters.length == 0) {
			return cond;
		}

		Checker.check(filters.length % 2 == 0, "filters must be key/value pairs like: \"lastName\", \"Banks\"");

		for (int i = 0; i < filters.length; i++) {
			if (i % 2 == 0) {
				cond.filter.put(filters[i].toString(), filters[i + 1]);
			}
		}

		return cond;
	}

	/**
	 * set Orders in this way:
	 *
	 * <code>
	 * Condition.filter().order("lastName", "ASC");
	 * </code>
	 *
	 * Overwrite previous orders.
	 *
	 *
	 * @param sorts
	 * @return
	 */
	public Condition sort(String... sorts) {

		if (sorts == null || sorts.length == 0) {
			return this;
		}

		Checker.check(sorts.length % 2 == 0, "orders must be field/order pairs like: \"_ts\", \"DESC\" ");

		this.sort = new ArrayList<>();
		for (int i = 0; i < sorts.length; i++) {
			if (i % 2 == 1) {
				Checker.check("ASC".equalsIgnoreCase(sorts[i]) || "DESC".equalsIgnoreCase(sorts[i]),
						String.format("Invalid order,expect: ASC / DESC, provided: %s", sorts[i]));
			}
			sort.add(sorts[i]);
		}

		return this;
	}

	/**
	 * Set select fields. Default is "*"
	 *
	 * <code>
	 * Condition.filter().fields("id", "name", "employeeCode");
	 * </code>
	 *
	 * Overwrites previous fields
	 *
	 * @param fields
	 * @return
	 */
	public Condition fields(String... fields) {

		if (fields == null || fields.length == 0) {
			return this;
		}

		this.fields = new LinkedHashSet<>(List.of(fields));
		return this;
	}

	public Condition offset(int offset) {
		this.offset = offset;
		return this;
	}

	public Condition limit(int limit) {
		this.limit = limit;
		return this;
	}

	public SqlQuerySpec toQuerySpec() {
		return toQuerySpec(false);
	}

	public SqlQuerySpec toQuerySpecForCount() {
		return toQuerySpec(true);
	}

	SqlQuerySpec toQuerySpec(boolean count) {

		var select = count ? "COUNT(1)" : generateSelect();

		var queryText = new StringBuilder(String.format("SELECT %s FROM c", select));
		var params = new SqlParameterCollection();

		// filter

		int conditionIndex = 0;

		AtomicInteger paramIndex = new AtomicInteger(0);

		for (var entry : this.filter.entrySet()) {

			var exp = Expression.parse(entry.getKey(), entry.getValue());

			if (conditionIndex == 0) {
				queryText.append(" WHERE");
			} else {
				queryText.append(" AND");
			}

			var expQuerySpec = exp.toQuerySpec(paramIndex);
			queryText.append(expQuerySpec.getQueryText());
			params.addAll(expQuerySpec.getParameters());

			conditionIndex++;

		}

		// sort
		if (!count && !CollectionUtils.isEmpty(sort) && sort.size() > 1) {

			var sortMap = new LinkedHashMap<String, String>();

			for (int i = 0; i < sort.size(); i++) {
				if (i % 2 == 0) {
					sortMap.put(sort.get(i), sort.get(i + 1));
				}
			}

			queryText.append(sortMap.entrySet().stream()
					.map(entry -> String.format(" c.%s %s", entry.getKey(), entry.getValue().toUpperCase()))
					.collect(Collectors.joining(",", " ORDER BY", "")));

		}

		// offset and limit
		if (!count) {
			queryText.append(String.format(" OFFSET %d LIMIT %d", offset, limit));
		}

		log.info("queryText:{}", queryText);

		return new SqlQuerySpec(queryText.toString(), params);

	}

	/**
	 * select parts generate.
	 *
	 * <pre>
	 * e.g.
	 * "id", "age", "fullName.first" -> VALUE {"id":c.id, "age":c.age, "fullName": {"first": c.fullName.first}}
	 * </pre>
	 *
	 * @return
	 */
	String generateSelect() {

		if (CollectionUtils.isEmpty(this.fields)) {
			return "*";
		}
		return this.fields.stream().map(f -> generateOneFieldSelect(f)).filter(Objects::nonNull)
				.collect(Collectors.joining(", ", "VALUE {", "}"));
	}

	/**
	 * generate a select for field.
	 *
	 * <pre>
	 * e.g.
	 * "name" -> "name": "c.name"
	 *
	 * "organization.leader.name" -> "organization": { "leader": {"name": c.organization.leader.name}}
	 *
	 * </pre>
	 *
	 * @see https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-working-with-json
	 *
	 * @param field
	 * @return
	 */
	static String generateOneFieldSelect(String field) {

		// empty field will be skipped
		if (StringUtils.isEmpty(field)) {
			return null;
		}

		if (StringUtils.containsAny(field, "{", "}", ",", "\"", "'")) {
			throw new IllegalArgumentException("field cannot contain '{', '}', ',', '\"', \"'\", field: " + field);
		}

		var fullField = "c." + field;

		// if not containing ".", return a simple json part
		if (!field.contains(".")) {
			return String.format("\"%s\":%s", field, fullField);
		}

		var parts = new ArrayDeque<>(List.of(field.split("\\.")));

		var map = createMap(parts, field);

		// "organization.leader.name" -> c.organization.leader.name
		var ret = JsonUtil.toJsonNoIndent(map).replace("\"" + field + "\"", fullField);

		ret = RegExUtils.removeFirst(ret, "\\{");
		ret = StringUtils.removeEnd(ret, "}");

		return ret;

	}

	/**
	 * Recursively create json object for select
	 * 
	 * @param parts
	 * @return
	 */
	static Map<String, Object> createMap(Deque<String> parts, String fullField) {
		var map = new LinkedHashMap<String, Object>();
		if (parts.size() <= 1) {
			map.put(parts.pop(), fullField);
			return map;
		}
		map.put(parts.pop(), createMap(parts, fullField));
		return map;
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

	/**
	 *
	 * BINARY_OPERATORの例： =, !=, >, >=, <, <= BINARY_FUCTIONの例： STARTSWITH,
	 * ENDSWITH, CONTAINS, ARRAY_CONTAINS
	 *
	 * @author zhang.lei
	 *
	 */
	public enum OperatorType {
		BINARY_OPERATOR, BINARY_FUNCTION
	}

	public static final Pattern expressionPattern = Pattern
			.compile("(.+)\\s(STARTSWITH|ENDSWITH|CONTAINS|ARRAY_CONTAINS|=|!=|<|<=|>|>=)\\s*$");

	public static final Pattern functionPattern = Pattern.compile("\\w+");

	public interface Expression {
		public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex);

		public static Expression parse(String key, Object value) {

			var m = expressionPattern.matcher(key);

			if (!m.find()) {
				if (key.contains(" OR ")) {
					return new OrExpressions(key, value);
				} else {
					return new SimpleExpression(key, value);
				}
			} else {
				if (key.contains(" OR ")) {
					return new OrExpressions(m.group(1), value, m.group(2));
				} else {
					return new SimpleExpression(m.group(1), value, m.group(2));
				}
			}
		}

		public static SimpleExpression toSimpleExpression(String key, Object value) {
			var exp = new SimpleExpression();
			exp.key = key;
			exp.value = value;
			return exp;
		}

	}

	public static class SimpleExpression implements Expression {

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
	}

	/**
	 * Expressions like "firstName OR lastName STARTSWITH" : "H"
	 *
	 * @author zhang.lei
	 *
	 */
	public static class OrExpressions implements Expression {

		public List<SimpleExpression> simpleExps = new ArrayList<>();

		public OrExpressions() {
		}

		public OrExpressions(List<SimpleExpression> simpleExps, Object value) {
			this.simpleExps = simpleExps;
		}

		public OrExpressions(String key, Object value) {
			var keys = key.split(" OR ");

			if (keys == null || keys.length == 0) {
				return;
			}
			this.simpleExps = List.of(keys).stream().map(k -> new SimpleExpression(k, value))
					.collect(Collectors.toList());
		}

		public OrExpressions(String key, Object value, String operator) {
			var keys = key.split(" OR ");

			if (keys == null || keys.length == 0) {
				return;
			}
			this.simpleExps = List.of(keys).stream().map(k -> new SimpleExpression(k, value, operator))
					.collect(Collectors.toList());
		}

		@Override
		public SqlQuerySpec toQuerySpec(AtomicInteger paramIndex) {

			var ret = new SqlQuerySpec();

			if (simpleExps == null || simpleExps.isEmpty()) {
				return ret;
			}

			var indexForQuery = paramIndex;
			var indexForParam = new AtomicInteger(paramIndex.get());

			var queryText = simpleExps.stream().map(exp -> exp.toQuerySpec(indexForQuery).getQueryText())
					.collect(Collectors.joining(" OR", " (", " )"));

			var params = simpleExps.stream().map(exp -> exp.toQuerySpec(indexForParam).getParameters())
					.reduce(new SqlParameterCollection(), (sum, elm) -> {
						sum.addAll(elm);
						return sum;
					});

			ret.setQueryText(queryText);
			ret.setParameters(params);

			return ret;

		}

	}

}
