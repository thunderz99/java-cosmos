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

/**
 * Condition for find. (e.g. filter / sort / offset / limit)
 */
public class Condition {

	private static Logger log = LoggerFactory.getLogger(Condition.class);

	/**
	 * Default constructor
	 */
	public Condition() {
	}

	public Map<String, Object> filter = new LinkedHashMap<>();

	public List<String> sort = List.of();

	public Set<String> fields = new LinkedHashSet<>();

	public int offset = 0;
	public int limit = 100;

	/**
	 * a raw query spec which can use raw sql
	 */
	public SqlQuerySpec rawQuerySpec = null;

	public static final String COND_SQL_TRUE = "1=1";
	public static final String COND_SQL_FALSE = "1=0";

	/**
	 * OperatorType for WHERE clause
	 *
	 * {@code
	 * BINARY_OPERATORの例：{@code =, !=, >, >=, <, <= }
	 * BINARY_FUCTIONの例： STARTSWITH, ENDSWITH, CONTAINS, ARRAY_CONTAINS
	 * }
	 *
	 */
	public enum OperatorType {
		BINARY_OPERATOR, BINARY_FUNCTION
	}

	public static final Pattern simpleExpressionPattern = Pattern
			.compile("(.+)\\s(STARTSWITH|ENDSWITH|CONTAINS|ARRAY_CONTAINS|LIKE|=|!=|<|<=|>|>=)\\s*$");

	public static final Pattern subQueryExpressionPattern = Pattern
			.compile("(.+)\\s(ARRAY_CONTAINS_ANY|ARRAY_CONTAINS_ALL)\\s*(.*)$");

	/**
	 * add filters
	 * @param filters search filters using key / value pair.
	 * @return condition
	 */
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
	 * set Orders in the following way. Overwrite previous orders.
	 *
	 * {@code
	 * Condition.filter().order("lastName", "ASC");
	 * }
	 *
	 * @param sorts sort strings
	 * @return condition
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
	 * Set select fields. Default is "*". Overwrites previous fields
	 *
	 * {@code
	 * Condition.filter().fields("id", "name", "employeeCode");
	 * }
	 *
	 * @param fields select fields
	 * @return condition
	 */
	public Condition fields(String... fields) {

		if (fields == null || fields.length == 0) {
			return this;
		}

		this.fields = new LinkedHashSet<>(List.of(fields));
		return this;
	}

	/**
	 * set the offset
	 * @param offset offset
	 * @return condition
	 */
	public Condition offset(int offset) {
		this.offset = offset;
		return this;
	}

	/**
	 * set the limit
	 * @param limit limit
	 * @return condition
	 */
	public Condition limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * Generate a query spec from condition.
	 * @return query spec which can be used in official DocumentClient
	 */
	public SqlQuerySpec toQuerySpec() {
		return toQuerySpec(false);
	}

	/**
	 * Generate a query spec for count from condition.
	 * @return query spec which can be used in official DocumentClient
	 */
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
					.map(entry -> String.format(" %s %s", getFormattedKey(entry.getKey()), entry.getValue().toUpperCase()))
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
	 * @param queryText queryText
	 * @param params params
	 */
	FilterQuery generateFilterQuery(StringBuilder queryText, SqlParameterCollection params,
			AtomicInteger conditionIndex, AtomicInteger paramIndex) {

		for (var entry : this.filter.entrySet()) {
			// filter parts
			var connectPart = getConnectPart(conditionIndex);

			var subFilterQueryToAdd = "";

			if (SubConditionType.SUB_COND_RAW.name().equals(entry.getKey())) {
				//raw condition. Used to generate always true/false cond. E.g. "1=1", "1=0"

				var rawText = entry.getValue().toString();
				if (StringUtils.isEmpty(rawText)) {
					continue;
				}

				subFilterQueryToAdd = connectPart + " (" + rawText + ")";
			} else if (SubConditionType.SUB_COND_AND.name().equals((entry.getKey()))){
				// sub query
				var value = entry.getValue();
				if (!(value instanceof List<?>)) {
					continue;
				}
				@SuppressWarnings("unchecked")
				var list = (List<Condition>) value;

				subFilterQueryToAdd = generateFilterQuery4List(list, "AND", connectPart, params, conditionIndex, paramIndex);


			} else if (SubConditionType.SUB_COND_OR.name().equals(entry.getKey())) {
				// sub query
				var value = entry.getValue();
				if (!(value instanceof List<?>)) {
					continue;
				}
				@SuppressWarnings("unchecked")
				var list = (List<Condition>) value;

				subFilterQueryToAdd = generateFilterQuery4List(list, "OR", connectPart, params, conditionIndex, paramIndex);
					
			} else {
				var exp = parse(entry.getKey(), entry.getValue());
				var expQuerySpec = exp.toQuerySpec(paramIndex);
				subFilterQueryToAdd = connectPart + expQuerySpec.getQueryText();
				params.addAll(expQuerySpec.getParameters());
			}

			queryText.append(subFilterQueryToAdd);
			conditionIndex.getAndIncrement();
		}

		return new FilterQuery(queryText, params, conditionIndex, paramIndex);
	}

	String generateFilterQuery4List(List<Condition> conds, String joiner, String connectPart, SqlParameterCollection params, AtomicInteger conditionIndex, AtomicInteger paramIndex) {
		List<String> subTexts = new ArrayList<>();

		for (var subCond : conds) {
			var subFilterQuery = subCond.generateFilterQuery(new StringBuilder(), params, conditionIndex,
					paramIndex);

			subTexts.add(removeConnectPart(subFilterQuery.queryText.toString()));

			params = subFilterQuery.params;
			conditionIndex = subFilterQuery.conditionIndex;
			paramIndex = subFilterQuery.paramIndex;
		}

		var subFilterQuery = subTexts.stream().filter(t -> StringUtils.isNotBlank(t))
				.collect(Collectors.joining(" " + joiner + " ", connectPart + " (", ")"));
		// remove empty sub queries
		return StringUtils.removeStart(subFilterQuery, connectPart + " ()");
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
	 * {@code
	 * e.g.
	 * "id", "age", "fullName.first" -> VALUE {"id":c.id, "age":c.age, "fullName": {"first": c.fullName.first}}
	 * }
	 *
	 * @return select sql
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
	 * {@code
	 * e.g.
	 * "name" -> "name": "c.name"
	 * "organization.leader.name" -> "organization": { "leader": {"name": c.organization.leader.name}}
	 * }
	 *
	 * @see <a href="https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-working-with-json">sql-query-working-with-json</a>
	 *
	 * @param field field name
	 * @return one field select sql
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
	 * @param parts parts splitted by "."
	 * @param fullField full field name: e.g. c.name.first
	 * @return a map for select. "organization.leader.name" -> c.organization.leader.name
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
	 * parse key and value to generate a valid expression
	 * @param key filter's key
	 * @param value filter's value
	 * @return expression for WHERE clause
	 */
	public static Expression parse(String key, Object value) {

		//simple expression
		var simpleMatcher = simpleExpressionPattern.matcher(key);
		if(simpleMatcher.find()){
			if (key.contains(" OR ")) {
				return new OrExpressions(simpleMatcher.group(1), value, simpleMatcher.group(2));
			} else {
				return new SimpleExpression(simpleMatcher.group(1), value, simpleMatcher.group(2));
			}
		}

		//subquery expression
		var subqueryMatcher = subQueryExpressionPattern.matcher(key);

		if(subqueryMatcher.find()){
			return new SubQueryExpression(subqueryMatcher.group(1), subqueryMatcher.group(3), value, subqueryMatcher.group(2));
		}

		//default key / value expression
		if (key.contains(" OR ")) {
			return new OrExpressions(key, value);
		} else {
			return new SimpleExpression(key, value);
		}
	}

	/**
	 * parse key and value to generate a simple expression (key = value)
	 * @param key filter's key
	 * @param value filter's value
	 * @return expression for WHERE clause
	 */
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
	 * @param queryText sql raw queryText
	 * @param params params used in sql
	 * @return condition
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
	 * @param queryText sql raw queryText
	 * @return condition
	 */
	public static Condition rawSql(String queryText) {
		var cond = new Condition();
		cond.rawQuerySpec = new SqlQuerySpec(queryText);
		return cond;
	}

	/**
	 * sub query 's OR / RAW operator
	 *
	 * <p>
	 * TODO SUB_COND_NOT operator
	 * </p>
	 */
	public enum SubConditionType {
		SUB_COND_AND, SUB_COND_OR, SUB_COND_RAW
	}

	/**
	 * Instead of c.key, return c["key"] or c["key1"]["key2"] for query. In order for cosmosdb reserved words
	 *
	 * @param key filter's key
	 * @return formatted filter's key c["key1"]["key2"]
	 */
	static String getFormattedKey(String key) {
		return getFormattedKey(key, "c");
	}

	/**
	 * Instead of c.key, return c["key"] or c["key1"]["key2"] for query. In order for cosmosdb reserved words
	 *
	 * @param key filter's key
	 * @param
	 * @return formatted filter's key c["key1"]["key2"]
	 */
	static String getFormattedKey(String key, String collectionAlias) {
		Checker.checkNotBlank(collectionAlias, "collectionAlias");

		if(StringUtils.isEmpty(key)){
			return collectionAlias;
		}

		var parts = key.split("\\.");
		var sb = new StringBuilder();
		sb.append(collectionAlias);
		for( var part : List.of(parts)){
			sb.append("[\"" + part + "\"]");
		}
		return sb.toString();
	}

	/**
	 * cond always true
	 *
	 * @return trueCondition
	 */
	public static Condition trueCondition() {
		return Condition.filter(SubConditionType.SUB_COND_RAW, COND_SQL_TRUE);
	}

	/**
	 * cond always false
	 *
	 * @return falseCondition
	 */
	public static Condition falseCondition() {
		return Condition.filter(SubConditionType.SUB_COND_RAW, COND_SQL_FALSE);
	}

	/**
	 * make a deep copy condition
	 *
	 * @return copy of condition
	 */
	public Condition copy() {
		var cond = new Condition();

		cond.filter = JsonUtil.toMap(JsonUtil.toJson(this.filter));
		cond.limit = this.limit;
		cond.offset = this.offset;
		cond.sort = new ArrayList<>(this.sort);
		cond.fields = new LinkedHashSet<>(this.fields);
		if (this.rawQuerySpec != null) {
			cond.rawQuerySpec = new SqlQuerySpec(this.rawQuerySpec.getQueryText(), this.rawQuerySpec.getParameters());
		}

		return cond;
	}
}
