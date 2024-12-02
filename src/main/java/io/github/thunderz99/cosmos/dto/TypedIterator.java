package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.CosmosDocumentIterator;

import java.util.Iterator;

/**
 * A typed iterator that wraps a CosmosDocumentIterator to provide a type-safe way to iterate over documents.
 * @param <T> the type of the objects in the iterator
 */
public class TypedIterator<T> implements Iterator<T> {

    CosmosDocumentIterator iterator;
    Class<T> clazz;

    TypedIterator(){};

    public TypedIterator(CosmosDocumentIterator iterator, Class<T> clazz){
        this.iterator = iterator;
        this.clazz = clazz;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
       return this.iterator.next(clazz);
    }
}
