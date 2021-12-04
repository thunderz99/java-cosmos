package io.github.thunderz99.cosmos.condition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;

/**
 * A helper class for SqlQuerySpec to serialize to/from json
 */
public class SqlQuerySpec4Json {

    public String queryText;

    public Map<String, Object> params = new LinkedHashMap<>();

    public SqlQuerySpec4Json() {
    }

    /**
     * create from SqlQuerySpec class
     *
     * @param querySpec querySpec
     */
    SqlQuerySpec4Json(SqlQuerySpec querySpec) {
        this.queryText = querySpec.getQueryText();
        this.params = querySpec.getParameters().stream().collect(Collectors.toMap( param -> param.getName(), param -> param.getValue(Object.class)));
    }

    /**
     * convert to SqlQuerySpec class
     *
     * @return querySpec
     */
    SqlQuerySpec toSqlQuerySpec() {
        var querySpec = new SqlQuerySpec();
        querySpec.setQueryText(this.queryText);
        var sqlParams = this.params.entrySet().stream().map(entry -> new SqlParameter(entry.getKey(), entry.getValue())).collect(Collectors.toList());
        querySpec.setParameters(sqlParams);
        return querySpec;
    }
}
