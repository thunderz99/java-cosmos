package io.github.thunderz99.cosmos.impl.cosmosdb;

import com.azure.cosmos.util.CosmosPagedIterable;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosDocumentIterator;

import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link CosmosDocumentIterator} interface, use to iterate {@link CosmosDocument} from a iterable of Map.
 *
 * <p>
 * {@link CosmosDocumentIterator} is an interface that represent an iterator of documents.
 * But the iterable is not a {@link Iterable<CosmosDocument>} but a {@link Iterable<Map>}.
 * So we need this class to convert the {@link Iterable<Map>} to a {@link Iterable<CosmosDocument>}
 * </p>
 */
public class CosmosDocumentIteratorImpl implements CosmosDocumentIterator {

    private Iterator<? extends Map> iterator;

    @Override
    public boolean hasNext() {
        if(iterator == null){
            return false;
        }
        return iterator.hasNext();
    }

    @Override
    public CosmosDocument next() {
        if(iterator == null){
            return null;
        }
        return new CosmosDocument(iterator.next());
    }

    public void setDocumentIterator(Iterator<? extends Map> iterator) {
        this.iterator = iterator;
    }
}
