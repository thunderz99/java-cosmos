package io.github.thunderz99.cosmos.dto;

import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A helper class that absorb the difference of SqlParameter(SDK v2) and SqlParameter(SDK v4), used to save name/value of a param
 */
public class CosmosSqlParameter extends RecordData {

    /**
     * name of param
     */
    public String name;

    /**
     * value of param (should be able to be json serialized)
     */
    public Object value;

    public CosmosSqlParameter(){
    }

    public CosmosSqlParameter(String name, Object value){
        this.name = name;
        this.value = value;
    }

    public CosmosSqlParameter(SqlParameter paramV2){
        this.name = paramV2.getName();
        this.value = paramV2.getValue(Object.class);
    }

    public CosmosSqlParameter(com.azure.cosmos.models.SqlParameter paramV4){
        this.name = paramV4.getName();
        this.value = paramV4.getValue(Object.class);
    }

    /**
     * Generate and return a SqlParameter V2 object
     * @return SqlParameter V2
     */
    @JsonIgnore
    public SqlParameter toSqlParameterV2(){
        return new SqlParameter(name, value);
    }

    /**
     * Generate and return a SqlParameter V4 object
     * @return SqlParameter V4
     */
    @JsonIgnore
    public com.azure.cosmos.models.SqlParameter toSqlParameterV4(){
        return new com.azure.cosmos.models.SqlParameter(name, value);
    }

    /**
     * Util method to generate a SqlParameterCollection(V2) from List of CosmosSqlParameter
     *
     * @param params params to convert
     * @return SqlParameterCollection
     */
    public static SqlParameterCollection toParamsV2(List<CosmosSqlParameter> params) {

        var ret = new SqlParameterCollection();

        if(CollectionUtils.isEmpty(params)){
            return ret;
        }

        ret.addAll(params.stream().map(p -> p.toSqlParameterV2()).collect(Collectors.toList()));
        return ret;
    }

    /**
     * Util method to generate a List of SqlParameter V4 from List of CosmosSqlParameter
     *
     * @param params params to convert
     * @return SqlParameterCollection
     */
    public static List<com.azure.cosmos.models.SqlParameter> toParamsV4(List<CosmosSqlParameter> params) {

        if(CollectionUtils.isEmpty(params)){
            return Lists.newArrayList();
        }

        return params.stream().map(p -> p.toSqlParameterV4()).collect(Collectors.toList());
    }


    /**
     * Return param name
     *
     * @return name
     */
    public String getName() {
        return this.name;
    }

    /**
     * Return param value
     *
     * @return value
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * Return the json string
     *
     * @return jsonString
     */
    @JsonIgnore
    public String toJson() {
        return this.toString();
    }


    /**
     * Return param value by key. Deprecated. Only for backwards compatibility
     *
     * <p>
     * return name only if input is "name".
     * return value only if input is "value".
     * return null if input is others.
     * </p>
     *
     * @param "name" or "value"
     * @return value by key
     */
    @Deprecated
    public Object get(String key) {
        if (StringUtils.equals(key, "name")) {
            return this.getName();
        }
        if (StringUtils.equals(key, "value")) {
            return this.getValue();
        }
        return null;
    }

    /**
     * Return param value(in String type) by key. Deprecated. Only for backwards compatibility
     *
     * <p>
     * return name only if input is "name". return value only if input is "value" <br/>
     * return null if input is others.
     * </p>
     *
     * @return value by key
     */
    @Deprecated
    public String getString(String key) {
        if (StringUtils.equals(key, "name")) {
            return this.getName();
        }
        if (StringUtils.equals(key, "value")) {
            return String.valueOf(this.getValue());
        }
        return null;
    }

}
