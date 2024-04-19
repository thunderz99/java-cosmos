package io.github.thunderz99.cosmos.dto;

import java.util.List;
import java.util.stream.Collectors;

import com.azure.cosmos.models.SqlParameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import org.apache.commons.collections4.CollectionUtils;

/**
 * A helper class that absorb the difference of SqlQuerySpec(SDK v2) and SqlQuerySpec(SDK v4), used to save the queryText and params.
 */
public class CosmosSqlQuerySpec extends RecordData {

    public String queryText = "";

    public List<CosmosSqlParameter> params = Lists.newArrayList();

    public CosmosSqlQuerySpec(){
    }

    public CosmosSqlQuerySpec(String queryText) {
        this.queryText = queryText;
    }

    public CosmosSqlQuerySpec(String queryText, List<?> params){
        this.queryText = queryText;

        if(!CollectionUtils.isEmpty(params)){

            var param = params.get(0);
            if(param instanceof CosmosSqlParameter){
                this.params = (List<CosmosSqlParameter>) params;
            } else if(param instanceof com.azure.cosmos.models.SqlParameter){
                this.params = ((List<com.azure.cosmos.models.SqlParameter>)params).stream().map(p -> new CosmosSqlParameter(p.getName(), p.getValue(Object.class)))
                        .collect(Collectors.toList());
            }

        }

    }


    CosmosSqlQuerySpec(com.azure.cosmos.models.SqlQuerySpec querySpecV4){
        this.queryText = querySpecV4.getQueryText();
        this.params = querySpecV4.getParameters().stream().map(p -> new CosmosSqlParameter(p.getName(), p.getValue(Object.class)))
                .collect(Collectors.toList());
    }

    public CosmosSqlQuerySpec(String queryText, SqlParameterCollection paramsV2){
        this.queryText = queryText;
        this.params = paramsV2.stream().map(p -> new CosmosSqlParameter(p.getName(), p.getValue(Object.class)))
                .collect(Collectors.toList());
    }


    /**
     * Generate and return a SqlQuerySpec V4 object
     * @return SqlQuerySpec V4
     */
    @JsonIgnore
    public com.azure.cosmos.models.SqlQuerySpec toSqlQuerySpecV4(){
        return new com.azure.cosmos.models.SqlQuerySpec(this.queryText, CosmosSqlParameter.toParamsV4(this.params));
    }

    /**
     * Get the parameters List
     * @return params
     */
    @JsonIgnore
    public List<CosmosSqlParameter> getParameters() {
        return params;
    }

    /**
     * Get the parameter collection(V2)
     * @return params
     */
    @JsonIgnore
    public SqlParameterCollection getParametersV2() {
        return CosmosSqlParameter.toParamsV2(params);
    }

    /**
     * Get the parameters List(V4)
     * @return params
     */
    @JsonIgnore
    public List<SqlParameter> getParametersv4() {
        return CosmosSqlParameter.toParamsV4(params);
    }

    /**
     * Get the query text string
     * @return queryText
     */
    public String getQueryText() {
        return queryText;
    }

    /**
     * set query text
     *
     * @param queryText query text to set
     */
    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    /**
     * set params for query
     *
     * @param params params to set
     */
    public void setParameters(List<CosmosSqlParameter> params){
        this.params = params;
    }
}

