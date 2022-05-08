package io.github.thunderz99.cosmos.rest;

import java.util.List;

import com.google.common.collect.Lists;
import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Cosmos db PATCH api's body bean
 */
public class PatchBody {

    /**
     * list of patch api operations
     */
    public List<Operation> operations = Lists.newArrayList();

    /**
     * Optional: condition in sql. e.g. "from c where c.address.zipCode ='98101' "
     */
    public String condition;

    /**
     * Default constructor
     */
    public PatchBody(){
    }

    /**
     * static constructor using operations
     * @param operations list of operations
     * @return patch body obj
     */
    public static PatchBody operations(List<Operation> operations){
        var body = new PatchBody();
        body.operations = operations;
        return body;
    }

    /**
     * static constructor using a single operation
     * @param operation single operation
     * @return patch body obj
     */
    public static PatchBody operation(Operation operation){
        var body = new PatchBody();
        body.operations.add(operation);
        return body;
    }

    /**
     * to string in json format
     * @return json string
     */
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
