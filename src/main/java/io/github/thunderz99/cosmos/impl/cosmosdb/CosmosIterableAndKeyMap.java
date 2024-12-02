package io.github.thunderz99.cosmos.impl.cosmosdb;

import com.azure.cosmos.util.CosmosPagedIterable;

import java.util.Map;


/**
 * A class that contains a paged iterable of documents and a map of key. The purpose of this class is to
 * store the result of a query and the key map in one object, used in a query with join.
 *
 * @author zhang.lei
 *
 */
public class CosmosIterableAndKeyMap {

    public CosmosPagedIterable<? extends Map> iterable;
    public Map<String, String[]> keyMap;

    public CosmosIterableAndKeyMap() {}

    public CosmosIterableAndKeyMap(CosmosPagedIterable<? extends Map> iterable, Map<String, String[]> keyMap) {
        this.iterable = iterable;
        this.keyMap = keyMap;
    }

}
