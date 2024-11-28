package io.github.thunderz99.cosmos;

import java.util.Iterator;

/**
 * Represent an iterator of CosmosDB document. Use this can suppress memory consumption compared to {@link CosmosDocumentList}
 *
 * <p>
 * Having hasNext() and next() method to iterate.
 * </p>
 */
public interface CosmosDocumentIterator extends Iterator<CosmosDocument> {

}
