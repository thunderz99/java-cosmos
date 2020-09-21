package io.github.thunderz99.cosmos.condition;

import java.util.ArrayDeque;
import java.util.ArrayList;
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

import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;

import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;

public class Condition {

	private static Logger log = LoggerFactory.getLogger(Condition.class);

	public Condition() {
	}

	public Map<String, Object> filter = new LinkedHashMap<>();

	public List<String> sort = List.of();

	public Set<String> fields = new LinkedHashSet<>();

	public int offset = 0;
	public int limit = 100;

	public SqlQuerySpec rawQuerySpec = null;

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

		// When rawSql is set, other filter / limit / offset / sort will be ignored.
		if (rawQuerySpec != null) {
			return rawQuerySpec;
		}

		var select = count ? "COUNT(1)" : generateSelect();

		var initialText = new StringBuilder(String.format("SELECT %s FROM c", select));
		var initialParams = new SqlParameterCollection();
		var initialConditionIndex = new AtomicInteger(0);
		var initialParamIndex = new AtomicInteger(0);

		var filterQuery = generateFilterQuery(initialText, initialParams, initialConditionIndex, initialParamIndex);

		var queryText = filterQuery.queryText;
		var params = filterQuery.params;

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
	 * filter parts
	 *
	 * @param queryText
	 * @param params
	 */
	FilterQuery generateFilterQuery(StringBuilder queryText, SqlParameterCollection params,
			AtomicInteger conditionIndex, AtomicInteger paramIndex) {

		for (var entry : this.filter.entrySet()) {
			// filter parts
			var connectPart = getConnectPart(conditionIndex);

			if (SubConditionType.SUB_COND_OR.name().equals(entry.getKey())) {
				// sub query
				var value = entry.getValue();
				if (!(value instanceof List<?>)) {
					continue;
				}
				@SuppressWarnings("unchecked")
				var list = (List<Condition>) value;

				List<String> subTexts = new ArrayList<>();

				for (var subCond : list) {
					var subFilterQuery = subCond.generateFilterQuery(new StringBuilder(), params, conditionIndex,
							paramIndex);

					subTexts.add(removeConnectPart(subFilterQuery.queryText.toString()));

					params = subFilterQuery.params;
					conditionIndex = subFilterQuery.conditionIndex;
					paramIndex = subFilterQuery.paramIndex;
				}

				var subFilterQueryText = subTexts.stream().collect(Collectors.joining(" OR ", connectPart + " (", ")"));
				queryText.append(subFilterQueryText);

			} else {
				var exp = parse(entry.getKey(), entry.getValue());
				var expQuerySpec = exp.toQuerySpec(paramIndex);
				queryText.append(connectPart + expQuerySpec.getQueryText());
				params.addAll(expQuerySpec.getParameters());
			}

			conditionIndex.getAndIncrement();
		}

		return new FilterQuery(queryText, params, conditionIndex, paramIndex);
	}

	private String removeConnectPart(String subQueryText) {
		return StringUtils.removeStart(StringUtils.removeStart(subQueryText, " WHERE"), " AND").trim();
	}

	static String getConnectPart(AtomicInteger conditionIndex) {
		return conditionIndex.get() == 0 ? " WHERE" : " AND";
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



	/**
	 * Use raw sql and params to do custom complex queries. When rawSql is set,
	 * other filter / limit / offset / sort will be ignored.
	 *
	 * @param queryText
	 * @param params
	 * @return
	 */
	public static Condition rawSql(String queryText, SqlParameterCollection params) {
		var cond = new Condition();
		cond.rawQuerySpec = new SqlQuerySpec(queryText, params);
		return cond;
	}

	/**
	 * Use raw sql to do custom complex queries. When rawSql is set, other filter /
	 * limit / offset / sort will be ignored.
	 *
	 * @param queryText
	 * @return
	 */
	public static Condition rawSql(String queryText) {
		var cond = new Condition();
		cond.rawQuerySpec = new SqlQuerySpec(queryText);
		return cond;
	}

	/**
	 * sub query 's OR / AND / NOT operator
	 *
	 * TODO SUB_COND_AND / SUB_COND_NOT operator
	 */
	public enum SubConditionType {
		SUB_COND_OR
	}

}
