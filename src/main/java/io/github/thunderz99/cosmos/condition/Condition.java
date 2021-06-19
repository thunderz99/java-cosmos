package io.github.thunderz99.cosmos.condition;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
	 * aggregate settings(function, groupBy)
	 */
	public Aggregate aggregate = null;

	/**
	 * whether this query is cross-partition or not (default to false)
	 */
	public boolean crossPartition = false;

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
			.compile("(.+)\\s(STARTSWITH|ENDSWITH|CONTAINS|ARRAY_CONTAINS|LIKE|IN|=|!=|<|<=|>|>=)\\s*$");

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
	 *
	 * @param limit limit
	 * @return condition
	 */
	public Condition limit(int limit) {
		this.limit = limit;
		return this;
	}

	/**
	 * set whether is cross-partition query
	 *
	 * @param crossPartition
	 * @return
	 */
	public Condition crossPartition(boolean crossPartition) {
		this.crossPartition = crossPartition;
		return this;
	}

	/**
	 * Generate a query spec from condition.
	 *
	 * @return query spec which can be used in official DocumentClient
	 */
	public SqlQuerySpec toQuerySpec() {
		return toQuerySpec(null);
	}

	/**
	 * Generate a query spec for count from condition.
	 * @return query spec which can be used in official DocumentClient
	 */
	public SqlQuerySpec toQuerySpecForCount() {
		var agg = Aggregate.function("COUNT(1)");
		return toQuerySpec(agg);
	}

	public SqlQuerySpec toQuerySpec(Aggregate aggregate) {

		// When rawSql is set, other filter / limit / offset / sort will be ignored.
		if (rawQuerySpec != null) {
			return rawQuerySpec;
		}

		var select = aggregate != null ? generateAggregateSelect(aggregate) : generateSelect();

		var initialText = new StringBuilder(String.format("SELECT %s FROM c", select));
		var initialParams = new SqlParameterCollection();
		var initialConditionIndex = new AtomicInteger(0);
		var initialParamIndex = new AtomicInteger(0);

		var filterQuery = generateFilterQuery(initialText, initialParams, initialConditionIndex, initialParamIndex);

		var queryText = filterQuery.queryText;
		var params = filterQuery.params;

		// group by
		if (aggregate != null && !CollectionUtils.isEmpty(aggregate.groupBy)) {
			var groupBy = aggregate.groupBy.stream().map(g -> getFormattedKey(g)).collect(Collectors.joining(", "));
			queryText.append(" GROUP BY ").append(groupBy);
		}

		// special logic for aggregate with cross-partition=true and sort is empty
		// We have to add a default sort to overcome a bug. 
		// see https://social.msdn.microsoft.com/Forums/en-US/535c7e4a-f5cb-4aa3-90f5-39a2c8024191/group-by-fails-for-crosspartition-queries?forum=azurecosmosdb

		if (this.crossPartition && CollectionUtils.isEmpty(sort) && aggregate != null && CollectionUtils.isNotEmpty(aggregate.groupBy)) {
			// use the groupBy's first field to sort
			sort = new ArrayList<String>();
			sort.add(aggregate.groupBy.stream().collect(Collectors.toList()).get(0));
			sort.add("ASC");
		}

		// sort
		if (!CollectionUtils.isEmpty(sort) && sort.size() > 1) {

			var sortMap = new LinkedHashMap<String, String>();

			for (int i = 0; i < sort.size(); i++) {
				if (i % 2 == 0) {
					sortMap.put(sort.get(i), sort.get(i + 1));
				}
			}

			var sorts = "";

			if(aggregate == null){
				sorts = sortMap.entrySet().stream()
				.map(entry -> String.format(" %s %s", getFormattedKey(entry.getKey()), entry.getValue().toUpperCase()))
				.collect(Collectors.joining(",", " ORDER BY", ""));

			} else if(!CollectionUtils.isEmpty(aggregate.groupBy)){
				//when GROUP BY, the ORDER BY must be added as an outer query
				/** e.g
				 *  SELECT * FROM (SELECT COUNT(1) AS facetCount, c.status, c.createdBy FROM c
				 *  WHERE c._partition = "Accounts" AND c.name LIKE "%Tom%" GROUP BY c.status, c.createdBy) agg ORDER BY agg.status
				 */
				queryText.insert(0, "SELECT * FROM (");
				//use "agg" as outer select clause's collection alias
				queryText.append(") agg");

				sorts = sortMap.entrySet().stream()
						.map(entry -> String.format(" %s %s", getFormattedKey(entry.getKey(), "agg"), entry.getValue().toUpperCase()))
						.collect(Collectors.joining(",", " ORDER BY", ""));
			} else {
				//we have aggregate but without groupBy, so just ignore sort
			}

			queryText.append(sorts);
		}

		// offset and limit
		if(aggregate == null || !CollectionUtils.isEmpty(aggregate.groupBy)) {
			//if not aggregate or not having group by, we do not need offset and  limit. Because the items will be only 1.
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

		// process raw sql
		if(this.rawQuerySpec != null){
			conditionIndex.getAndIncrement();
			params.addAll(this.rawQuerySpec.getParameters());
			return new FilterQuery(new StringBuilder(this.rawQuerySpec.getQueryText()),
					this.rawQuerySpec.getParameters(), conditionIndex, paramIndex);
		}

		// process filters
		for (var entry : this.filter.entrySet()) {

			if (StringUtils.isEmpty(entry.getKey())) {
				// ignore when key is empty
				continue;
			}

			// filter parts
			var connectPart = getConnectPart(conditionIndex);

			var subFilterQueryToAdd = "";

			if (entry.getKey().startsWith(SubConditionType.SUB_COND_AND.name())) {
				// sub query AND
				var subQueries = extractSubQueries(entry.getValue());
				subFilterQueryToAdd = generateFilterQuery4List(subQueries, "AND", connectPart, params, conditionIndex, paramIndex);

			} else if (entry.getKey().startsWith(SubConditionType.SUB_COND_OR.name())) {
				// sub query OR
				var subQueries = extractSubQueries(entry.getValue());
				subFilterQueryToAdd = generateFilterQuery4List(subQueries, "OR", connectPart, params, conditionIndex, paramIndex);

			} else {
				// normal expression
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

	/**
	 * extract subQueries for SUB_COND_AND / SUB_COND_OR 's filter value
	 *
	 * @param value
	 */
	static List<Condition> extractSubQueries(Object value) {
		if (value == null) {
			return List.of();
		}

		if (value instanceof Condition) {
			return List.of((Condition) value);
		} else if (value instanceof List<?>) {
			return (List<Condition>) value;
		}

		return List.of();

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
	 * generate select parts for aggregate
	 * @param aggregate
	 * @return
	 */
	static String generateAggregateSelect(Aggregate aggregate) {

		if (aggregate == null) {
			throw new IllegalArgumentException("aggregate and function cannot be empty");
		}

		var select = new ArrayList<String>();

		if (StringUtils.isNotEmpty(aggregate.function)) {
			select.add(aggregate.function);
		}

		if (CollectionUtils.isNotEmpty(aggregate.groupBy)) {
			select.addAll(aggregate.groupBy.stream().map(f -> getFormattedKey(f)).filter(Objects::nonNull).collect(Collectors.toList()));
		}

		if (select.isEmpty()) {
			throw new IllegalArgumentException("aggregate and function cannot both be empty");
		}

		return select.stream().collect(Collectors.joining(", "));
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
		SUB_COND_AND, SUB_COND_OR
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
		return Condition.rawSql(COND_SQL_TRUE);
	}

	/**
	 * cond always false
	 *
	 * @return falseCondition
	 */
	public static Condition falseCondition() {
		return Condition.rawSql(COND_SQL_FALSE);
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
		cond.crossPartition = this.crossPartition;
		if (this.rawQuerySpec != null) {
			cond.rawQuerySpec = new SqlQuerySpec(this.rawQuerySpec.getQueryText(), this.rawQuerySpec.getParameters());
		}

		return cond;
	}

	/**
	 * fix enum exception in documentdb sdk
	 *
	 * @param key   key of param
	 * @param value value of param (enum will automatically convert to string by .name())
	 * @return a SqlParameter instance created
	 */
	public static SqlParameter createSqlParameter(String key, Object value) {
		if (value instanceof Enum<?>) {
			value = ((Enum<?>) value).name();
		}

		return new SqlParameter(key, value);

	}

	/**
	 * judge whether the condition is a trueCondition
	 *
	 * @param cond condition to judge
	 * @return true/false
	 */
	public static boolean isTrueCondition(Condition cond) {
		return cond.rawQuerySpec != null && COND_SQL_TRUE.equals(cond.rawQuerySpec.getQueryText());
	}

	/**
	 * judge whether the condition is a falseCondition
	 *
	 * @param cond condition to judge
	 * @return true/false
	 */
	public static boolean isFalseCondition(Condition cond) {
		return cond.rawQuerySpec != null && COND_SQL_FALSE.equals(cond.rawQuerySpec.getQueryText());
	}
}
