package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.CosmosDocumentIterator;

import java.util.Iterator;
import java.util.Map;


/**
 * A wrapper class for {@link CosmosDocumentIterator} to iterate over a collection of map.
 */
public class MapIterator implements Iterator<Map<String, Object>> {

    CosmosDocumentIterator iterator;

    MapIterator(){};

    public MapIterator(CosmosDocumentIterator iterator){
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Map<String, Object> next() {
       return this.iterator.next().toMap();
    }
}
