package io.github.thunderz99.cosmos.condition;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Condition for find. (e.g. filter / sort / offset / limit)
 *
 * <p>
 * for details, @see <a href="https://github.com/thunderz99/java-cosmos#complex-queries">github README</a>
 * </p>
 */
public class Condition {

    private static Logger log = LoggerFactory.getLogger(Condition.class);

    /**
     * Default constructor
     */
    public Condition() {
    }

    /**
     * A constructor accepting a map as filter
     *
     * @param filter map as filter directly
     */
    public Condition(Map<String, Object> filter) {
        this.filter = filter;
    }

    public Map<String, Object> filter = new LinkedHashMap<>();

    public Set<String> join = new LinkedHashSet<>();

    public Map<String,List<String>> joinCondText =new HashMap<>();

    public boolean returnAllSubArray = true;
    public List<String> sort = List.of();

    public Set<String> fields = new LinkedHashSet<>();

    public int offset = 0;
    public int limit = 100;

    /**
     * whether this query is cross-partition or not (default to false)
     */
    public boolean crossPartition = false;

    /**
     * whether this query is a negative query. (default to false)
     * <p>
     * this field represents the NOT operator in cosmos db
     * </p>
     * <p>
     * attention. this field will have no effect when this condition is a complete rawSql
     * </p>
     */
    public boolean negative = false;

    /**
     * a raw query spec which can use raw sql
     */
    public CosmosSqlQuerySpec rawQuerySpec = null;

    public static final String COND_SQL_TRUE = "1=1";
    public static final String COND_SQL_FALSE = "1=0";

    /**
     * OperatorType for WHERE clause
     * <p>
     * {@code
     * BINARY_OPERATORの例：{@code =, !=, >, >=, <, <= }
     * BINARY_FUCTIONの例： STARTSWITH, ENDSWITH, CONTAINS, ARRAY_CONTAINS
     * }
     */
    public enum OperatorType {
        BINARY_OPERATOR, BINARY_FUNCTION
    }

    public static final String TYPE_CHECK_FUNCTIONS = "IS_ARRAY|IS_BOOL|IS_DEFINED|IS_NULL|IS_NUMBER|IS_OBJECT|IS_PRIMITIVE|IS_STRING";


    public static final Pattern simpleExpressionPattern = Pattern
            .compile("(.+)\\s(STARTSWITH|ENDSWITH|CONTAINS|ARRAY_CONTAINS|LIKE|IN|RegexMatch|" + TYPE_CHECK_FUNCTIONS + "|=|!=|<|<=|>|>=)\\s*$");

    public static final Pattern typeCheckFunctionPattern = Pattern
            .compile(TYPE_CHECK_FUNCTIONS);

    public static final Pattern subQueryExpressionPattern = Pattern
            .compile("(.+)\\s(ARRAY_CONTAINS_ANY|ARRAY_CONTAINS_ALL)\\s*(.*)$");

    /**
     * add filters
     *
     * <p>
     * for details, @see <a href="https://github.com/thunderz99/java-cosmos#complex-queries">github README</a>
     * </p>
     *
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
     * set the join
     *
     * @param join join set
     * @return condition
     */
    public Condition join(Set<String> join) {
        this.join = join;
        return this;
    }

    /**
     * If it is true, return all the result in the sub array.
     * This function works only when join is used.
     * @param returnAllSubArray default value is true
     * @return condition
     */
    public Condition returnAllSubArray(boolean returnAllSubArray) {
        this.returnAllSubArray = returnAllSubArray;
        return this;
    }

    /**
     * set whether is cross-partition query
     *
     * @param crossPartition whether a crossPartition query or not
     * @return condition
     */
    public Condition crossPartition(boolean crossPartition) {
        this.crossPartition = crossPartition;
        return this;
    }

    /**
     * set the query to a NOT query
     *
     * <p>
     * this is a toggle function. if you do twice not(), the result is the same as no not().
     * </p>
     *
     * @return condition
     */
    public Condition not() {
        this.negative = !this.negative;
        return this;
    }

    /**
     * Generate a query spec from condition.
     *
     * @return query spec which can be used in official DocumentClient
     */
    public CosmosSqlQuerySpec toQuerySpec() {
        // When rawSql is set, other filter / limit / offset / sort will be ignored.
        if (rawQuerySpec != null) {
            return rawQuerySpec;
        }

        var select = generateSelect();

        var initialText = String.format("SELECT %s FROM c", select);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var filterQuery = generateFilterQuery(initialText, initialParams, initialConditionIndex, initialParamIndex, "c");

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // sort
        if (!CollectionUtils.isEmpty(sort) && sort.size() > 1) {
            var sortMap = new LinkedHashMap<String, String>();
            for (int i = 0; i < sort.size(); i++) {
                if (i % 2 == 0) {
                    sortMap.put(sort.get(i), sort.get(i + 1));
                }
            }
            var sorts = sortMap.entrySet().stream()
                    .map(entry -> String.format(" %s %s", getFormattedKey(entry.getKey()), entry.getValue().toUpperCase()))
                    .collect(Collectors.joining(",", " ORDER BY", ""));

            queryText.append(sorts);
        }

        // offset and limit
        queryText.append(String.format(" OFFSET %d LIMIT %d", offset, limit));

        log.info("queryText:{}", queryText);

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }

    /**
     * Generate a query spec for count from condition.
     *
     * @return query spec which can be used in official DocumentClient
     */
    public CosmosSqlQuerySpec toQuerySpecForCount() {
        var agg = Aggregate.function("COUNT(1)");
        return toQuerySpecForAggregate(agg);
    }

    /**
     * Generate a query spec for aggregation
     *
     * @param aggregate
     * @return query spec that do aggregation
     */
    public CosmosSqlQuerySpec toQuerySpecForAggregate(Aggregate aggregate) {

        Checker.checkNotNull(aggregate, "aggregate");

        // When rawSql is set, other filter / limit / offset / sort / aggregate will be ignored.
        if (rawQuerySpec != null) {
            return rawQuerySpec;
        }

        var select = generateAggregateSelect(aggregate);

        var initialText = String.format("SELECT %s FROM c", select);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var filterQuery = generateFilterQuery(initialText, initialParams, initialConditionIndex, initialParamIndex, "c");

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // group by
        if (!CollectionUtils.isEmpty(aggregate.groupBy)) {
            var groupBy = aggregate.groupBy.stream().map(g -> getFormattedKey(g)).collect(Collectors.joining(", "));
            queryText.append(" GROUP BY ").append(groupBy);
        }

        // sort (inner sort will be ignored for aggregate)
        // no need to deal with

        // offset and limit will be set and the following condition
        // 1. groupBy is enabled
        // 2. outer query is null. if outer query is enabled, setting inner offset / limit will cause sql exception in cosmosdb
        if (CollectionUtils.isNotEmpty(aggregate.groupBy) && aggregate.condAfterAggregate == null) {
            queryText.append(String.format(" OFFSET %d LIMIT %d", offset, limit));
        }

        // condition after aggregation
        FilterQuery filterQueryAgg = null;
        if (aggregate.condAfterAggregate != null) {

            // only works when groupBy is enabled
            if (CollectionUtils.isNotEmpty(aggregate.groupBy)) {

                var condAfter = aggregate.condAfterAggregate;
                // select

                //After GROUP BY, the WHERE / ORDER BY / LIMIT must be added as an outer query
                /** e.g
                 *  SELECT * FROM (SELECT COUNT(1) AS facetCount, c.status, c.createdBy FROM c
                 *  WHERE c._partition = "Accounts" AND c.name LIKE "%Tom%" GROUP BY c.status, c.createdBy) agg ORDER BY agg.status
                 */
                queryText.insert(0, "SELECT * FROM (");
                //use "agg" as outer select clause's collection alias
                queryText.append(") agg");

                // filter after agg

                var initialConditionIndexAgg = new AtomicInteger();

                filterQueryAgg = condAfter.generateFilterQuery(queryText.toString(), params, initialConditionIndexAgg, initialParamIndex, "agg");

                // special logic for aggregate with cross-partition=true and sort is empty
                // We have to add a default sort to overcome a bug.
                // see https://social.msdn.microsoft.com/Forums/en-US/535c7e4a-f5cb-4aa3-90f5-39a2c8024191/group-by-fails-for-crosspartition-queries?forum=azurecosmosdb


                if (this.crossPartition && CollectionUtils.isEmpty(condAfter.sort)) {
                    // use the groupBy's first field to sort
                    condAfter.sort = new ArrayList<>();
                    condAfter.sort.add(aggregate.groupBy.stream().collect(Collectors.toList()).get(0));
                    condAfter.sort.add("ASC");
                }

                // sort after agg
                // Note that only field like "status" "name" can be sort after group by.
                // aggregation value like "count" cannot be used in sort after group by.
                if (!CollectionUtils.isEmpty(condAfter.sort) && condAfter.sort.size() > 1) {
                    var sortMap = new LinkedHashMap<String, String>();

                    for (int i = 0; i < condAfter.sort.size(); i++) {
                        if (i % 2 == 0) {
                            sortMap.put(condAfter.sort.get(i), condAfter.sort.get(i + 1));
                        }
                    }
                    var sorts = sortMap.entrySet().stream()
                            .map(entry -> String.format(" %s %s", getFormattedKey(entry.getKey(), "agg"), entry.getValue().toUpperCase()))
                            .collect(Collectors.joining(",", " ORDER BY", ""));

                    filterQueryAgg.queryText.append(sorts);
                }

                // offset and limit after agg
                filterQueryAgg.queryText.append(String.format(" OFFSET %d LIMIT %d", condAfter.offset, condAfter.limit));
            }

        }

        if (filterQueryAgg != null) {
            queryText = filterQueryAgg.queryText;
            params = filterQueryAgg.params;
        }

        log.info("queryText:{}", queryText);

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }


    /**
     * filter parts
     *
     * @param selectPart  queryText
     * @param params      params
     * @param selectAlias "c.xxx"
     */
    FilterQuery generateFilterQuery(String selectPart, List<CosmosSqlParameter> params,
                                    AtomicInteger conditionIndex, AtomicInteger paramIndex, String selectAlias) {

        // process raw sql
        if (this.rawQuerySpec != null) {
            conditionIndex.getAndIncrement();
            params.addAll(this.rawQuerySpec.getParameters());
            String rawQueryText = processNegativeQuery(this.rawQuerySpec.getQueryText(), this.negative);
            return new FilterQuery(rawQueryText,
                    params, conditionIndex, paramIndex);
        }

        // process filters

        var queryTexts = new ArrayList<String>();

        // filter parts
        var connectPart = getConnectPart(conditionIndex);

        for (var entry : this.filter.entrySet()) {

            if (StringUtils.isEmpty(entry.getKey())) {
                // ignore when key is empty
                continue;
            }

            var subFilterQueryToAdd = "";

            if (entry.getKey().startsWith(SubConditionType.AND)) {
                // sub query AND
                var subQueries = extractSubQueries(entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "AND", params, conditionIndex, paramIndex);

            } else if (entry.getKey().startsWith(SubConditionType.OR)) {
                // sub query OR
                var subQueries = extractSubQueries(entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "OR", params, conditionIndex, paramIndex);

            } else if (entry.getKey().startsWith(SubConditionType.NOT)) {
                // sub query NOT
                var subQueries = extractSubQueries(entry.getValue());
                var subQueryWithNot = Condition.filter(SubConditionType.AND, subQueries).not();
                // recursively generate the filterQuery with negative flag true
                var filterQueryWithNot = subQueryWithNot.generateFilterQuery("", params, conditionIndex, paramIndex, selectAlias);
                subFilterQueryToAdd = " " + removeConnectPart(filterQueryWithNot.queryText.toString());
                saveOriginJoinCondition(subFilterQueryToAdd);
                subFilterQueryToAdd=toJoinQueryText( subFilterQueryToAdd,  subFilterQueryToAdd,  paramIndex);
            } else {
                // normal expression
                var exp = parse(entry.getKey(), entry.getValue());
                var expQuerySpec = exp.toQuerySpec(paramIndex, selectAlias);
                subFilterQueryToAdd = expQuerySpec.getQueryText();
                saveOriginJoinCondition(subFilterQueryToAdd);
                subFilterQueryToAdd = toJoinQueryText(entry.getKey(), subFilterQueryToAdd,paramIndex);
                params.addAll(expQuerySpec.getParameters());
            }

			if (StringUtils.isNotEmpty(subFilterQueryToAdd)) {
				queryTexts.add(subFilterQueryToAdd);
				conditionIndex.getAndIncrement();
			}
		}
		var queryText = String.join(" AND", queryTexts);

		queryText = processNegativeQuery(queryText, this.negative);

		//add WHERE part
		if (StringUtils.isNotEmpty(queryText)) {
			queryText = connectPart + queryText;
		}

		//add SELECT part
		queryText = selectPart + queryText;

		return new FilterQuery(queryText, params, conditionIndex, paramIndex);
	}

    private String toJoinQueryText(String key, String subFilterQueryToAdd, AtomicInteger paramIndex) {
        for (String joinPart : this.join) {
            if(key.contains(joinPart) ||subFilterQueryToAdd.contains(getFormattedKey(joinPart))){
                var newAlias="j"+paramIndex;
                var newParam=subFilterQueryToAdd.replace(getFormattedKey(joinPart),newAlias);
                var mainPart=getFormattedKey(joinPart);
                subFilterQueryToAdd=String.format(" EXISTS( SELECT VALUE %s FROM %s IN %s WHERE %s)",newAlias,newAlias,mainPart,newParam);
                break;
            }
        }

        return subFilterQueryToAdd;
    }

    /**
     * Save the conditions of the join part to map.
     * @param originJoinConditionText condition text
     */
    private void saveOriginJoinCondition(String originJoinConditionText){
        for (String joinPart : this.join) {
            if(originJoinConditionText.contains(getFormattedKey(joinPart))){
                var joinCondTextList= joinCondText.getOrDefault(joinPart,new ArrayList<>());
                joinCondTextList.add(originJoinConditionText);
                joinCondText.put(joinPart,joinCondTextList);

                break;
            }
        }
    }

	/**
	 * add negative NOT operator for queryText, if not empty
	 *
	 * @param queryText
	 * @param negative
	 * @return
	 */
	static String processNegativeQuery(String queryText, boolean negative) {
		return negative && StringUtils.isNotEmpty(queryText) ?
				" NOT(" + queryText + ")" : queryText;
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

        if (value instanceof Condition || value instanceof Map<?, ?>) {
            // single condition
            return List.of(extractSubQuery(value));
        } else if (value instanceof List<?>) {
            // multi condition
            var listValue = (List<Object>) value;
            return listValue.stream().map(v -> extractSubQuery(v)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        return List.of();
    }

    /**
     * extract subQuery for SUB_COND_AND / SUB_COND_OR 's filter value, single condition only.
     *
     * @param value
     */
    static Condition extractSubQuery(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Condition) {
            // single condition
            return (Condition) value;
        } else if (value instanceof Map<?, ?>) {
            // single condition in the form of map
            return new Condition(JsonUtil.toMap(value));
        } else if (value instanceof Collection<?>) {
            throw new IllegalArgumentException("Cannot convert input to a single condition. Ensure the input is a single value(not a collection)." + value);
        }
        throw new IllegalArgumentException("Invalid input. expect a condition or a map. " + value);
    }


    /**
     * @param conds          conditions
     * @param joiner         "AND", "OR"
     * @param params         sql params
     * @param conditionIndex increment index for conditions (for uniqueness of param names)
     * @param paramIndex     increment index for params (for uniqueness of param names)
     * @return query text
     */
    String generateFilterQuery4List(List<Condition> conds, String joiner, List<CosmosSqlParameter> params, AtomicInteger conditionIndex, AtomicInteger paramIndex) {
        List<String> subTexts = new ArrayList<>();
        List<String> originSubTexts = new ArrayList<>();

        for (var subCond : conds) {
            var subFilterQuery = subCond.generateFilterQuery("", params, conditionIndex,
                    paramIndex, "c");

            var originSubText = removeConnectPart(subFilterQuery.queryText.toString());
            subTexts.add(toJoinQueryText(originSubText, originSubText, paramIndex));
            originSubTexts.add(originSubText);
            params = subFilterQuery.params;
            conditionIndex = subFilterQuery.conditionIndex;
            paramIndex = subFilterQuery.paramIndex;
        }

        var subFilterQuery = subTexts.stream().filter(t -> StringUtils.isNotBlank(t))
                .collect(Collectors.joining(" " + joiner + " ", " (", ")"));

        var originSubFilterQuery = originSubTexts.stream().filter(t -> StringUtils.isNotBlank(t))
                .collect(Collectors.joining(" " + joiner + " ", " (", ")"));

        saveOriginJoinCondition(StringUtils.removeStart(originSubFilterQuery, " ()"));

        // remove empty sub queries
        return StringUtils.removeStart(subFilterQuery, " ()");
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
		return generateSelectByFields(this.fields);
	}

	/**
	 * Generate a select sql for input fields. Supports nested fields
	 *
	 * <p>
	 * {@code
	 * //e.g.
	 * //input: ["id", "contents.sheet-1.name", "contents.sheet-1.age", "contents.sheet-2.address"]
	 * //output: VALUE {"id":c.id, "contents":{"sheet-1": {"name": c["contents"]["sheet-1"]["name"],c["contents"]["sheet-1"]["age"] },"sheet-2":{"address": c["contents"]["sheet-2"]["address"]}}}
	 * }
	 *
	 * </p>
	 *
	 * @param fields
	 * @return
	 */
	static String generateSelectByFields(Set<String> fields) {

		Map<String, Object> fieldMap = Maps.newLinkedHashMap();

		for (var field : fields) {

			if (StringUtils.containsAny(field, "{", "}", ",", "\"", "'")) {
				throw new IllegalArgumentException("field cannot contain '{', '}', ',', '\"', \"'\", field: " + field);
			}
			// skip empty fields
			if (StringUtils.isEmpty(field)) {
				continue;
			}
			var parts = new ArrayDeque<String>();
			if (!field.contains(".")) {
				parts.add(field);
			} else {
				parts.addAll(List.of(field.split("\\.")));
			}

			fieldMap = addFieldToMap(fieldMap, parts, "c." + field);
		}

		var ret = JsonUtil.toJsonNoIndent(fieldMap);

		for (var field : fields) {
			ret = ret.replace("\"c." + field + "\"", getFormattedKey(field));
		}

		return "VALUE " + ret;
	}

	/**
	 * process and add a single field to the fieldMap
	 *
	 * @param fieldMap  global map object containing field struction map
	 * @param parts     parts consist of a field e.g: ["contents", "sheet-1", "name"]);
	 * @param fullField fieldName starts with collection. e.g: c.contents.sheet-1.name
	 */
	static Map<String, Object> addFieldToMap(Map<String, Object> fieldMap, ArrayDeque<String> parts, String fullField) {

		if (CollectionUtils.isEmpty(parts) || StringUtils.isEmpty(fullField)) {
			return fieldMap;
		}

		if (parts.size() == 1) {
			fieldMap.put(parts.pop(), fullField);
			return fieldMap;
		}

		var part = parts.pop();

		// process the part which has size >= 2
		var value = fieldMap.get(part);
		Map<String, Object> subMap = null;
		if (value == null) {
			subMap = new LinkedHashMap<String, Object>();
			fieldMap.put(part, subMap);
		} else if (value instanceof String) {
			// do nothing is already a String type, which means an end to the part.
			return fieldMap;
		} else if (value instanceof Map<?, ?>) {
			subMap = (Map<String, Object>) value;
		}

		// recursively process remaining parts
		subMap = addFieldToMap(subMap, parts, fullField);
		fieldMap.put(part, subMap);

		return fieldMap;
	}

	/**
	 * generate select parts for aggregate
	 *
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
        if (simpleMatcher.find()) {
            if (key.contains(" OR ")) {
                return new OrExpressions(simpleMatcher.group(1), value, simpleMatcher.group(2));
            } else {
                return new SimpleExpression(simpleMatcher.group(1), value, simpleMatcher.group(2));
            }
        }

        //subquery expression
        var subqueryMatcher = subQueryExpressionPattern.matcher(key);

        if (subqueryMatcher.find()) {
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
     *
     * @param key   filter's key
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
     * @param params    params used in sql
     * @return condition
     */
    public static Condition rawSql(String queryText, SqlParameterCollection params) {
        var cond = new Condition();
        cond.rawQuerySpec = new CosmosSqlQuerySpec(queryText, params);
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
        cond.rawQuerySpec = new CosmosSqlQuerySpec(queryText);
        return cond;
    }

    /**
     * Instead of c.key, return c["key"] or c["key1"]["key2"] for query. In order for cosmosdb reserved words
     *
     * @param key filter's key
     * @return formatted filter's key c["key1"]["key2"]
     */
    public static String getFormattedKey(String key) {
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

        if (StringUtils.isEmpty(key)) {
            return collectionAlias;
        }

        var parts = key.split("\\.");
        var sb = new StringBuilder();
        sb.append(collectionAlias);
        for (var part : List.of(parts)) {
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
        cond.filter = copyFilter(this.filter);

        cond.offset = this.offset;
        cond.limit = this.limit;
        cond.sort = new ArrayList<>(this.sort);
        cond.fields = new LinkedHashSet<>(this.fields);
        cond.crossPartition = this.crossPartition;
        cond.negative = this.negative;

        if (this.rawQuerySpec != null) {
            cond.rawQuerySpec = this.rawQuerySpec.copy();
        }

        return cond;
    }

	Map<String, Object> copyFilter(Map<String, Object> filter) {

		if (filter == null) {
			return filter;
		}
		
		var ret = new LinkedHashMap<String, Object>();
		for (var entry : filter.entrySet()) {
			var key = entry.getKey();
			var value = entry.getValue();
			ret.put(key, copyValue(value));
		}

		return ret;
	}

	/**
	 * deep copy the value according to value's class
	 *
	 * @param value
	 * @return copied value
	 */
	Object copyValue(Object value) {

		if (value == null) {
			return value;
		}

		// if value is a condition, copy the nested condition
		if (value instanceof Condition) {
			return ((Condition) value).copy();
		}

		// if value is a collection
		if (value instanceof Collection<?>) {
			var coll = (Collection<?>) value;
			return coll.stream().map(item -> copyValue(item)).collect(Collectors.toList());
		}

        // primitive type
        if (value instanceof String || Primitives.isWrapperType(value.getClass()) || value.getClass().isPrimitive() || value.getClass().isEnum()) {
            return value;
        }

        return JsonUtil.toMap(value);

    }

    /**
     * fix enum exception in documentdb sdk
     *
     * @param key   key of param
     * @param value value of param (enum will automatically convert to string by .name())
     * @return a SqlParameter instance created
     */
    public static CosmosSqlParameter createSqlParameter(String key, Object value) {
        if (value instanceof Enum<?>) {
            value = ((Enum<?>) value).name();
        }

        return new CosmosSqlParameter(key, value);

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

    /**
     * A getter in order to serialize rawQuerySpec to json string
     *
     * <p>
     * if rawQuerySpec is null, returns null
     * </p>
     *
     * @return a json string representing rawQuerySpec
     */
    public String getRawQuerySpecJson() {
        if (rawQuerySpec == null) {
            return null;
        }
        return JsonUtil.toJson(this.rawQuerySpec);
    }

    /**
     * Return a FieldKey obj representing a keyName in json documents of db
     *
     * @param keyName
     * @return fieldKey obj
     */
    public static FieldKey key(String keyName) {
        return new FieldKey(keyName);
    }


}
