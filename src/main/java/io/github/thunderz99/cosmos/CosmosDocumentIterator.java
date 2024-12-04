package io.github.thunderz99.cosmos;

import io.github.thunderz99.cosmos.dto.MapIterator;

import java.util.Iterator;

/**
 * Represent an iterator of CosmosDB document. Use this can suppress memory consumption compared to {@link CosmosDocumentList}
 *
 * <p>
 * Having hasNext() and next() method to iterate.
 * </p>
 */
public interface CosmosDocumentIterator extends Iterator<CosmosDocument> {

    /**
     * @param clazz
     * @return the next object
     * @param <T>
     */
    public <T> T next(Class<T> clazz);

    /**
     * Get a typed iterator that can be used to iterate over the documents in
     * the current iterator. The elements of the iterator will be of type {@code T}.
     * @param clazz the class of the elements in the iterator.
     * @return a typed iterator.
     * @param <T> the type of the elements in the iterator.
     */
    public <T> Iterator<T> getTypedIterator(Class<T> clazz);

    /**
     * Return a {@link MapIterator} that can be used to iterate over the documents in this iterator.
     *
     * <p>
     * {@code
     * // The elements of the iterator will be of type
     * Map<String, Object>
     * }
     * </p>
     *
     * @return a map iterator.
     */
    public MapIterator getMapIterator();

}
