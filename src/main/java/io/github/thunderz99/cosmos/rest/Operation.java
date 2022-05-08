package io.github.thunderz99.cosmos.rest;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Cosmos db PATCH api's operation bean
 */
public class Operation {

    public String op;
    public String path;
    public Object value;

    /**
     * Default constructor
     */
    public Operation(){
    }

    /**
     * Constructor using op / path / value
     * @param op
     * @param path
     * @param value
     */
    public Operation(String op, String path, Object value){
        this.op = op;
        this.path = path;
        this.value = value;
    }

    /**
     * to string in json format
     * @return json string
     */
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
