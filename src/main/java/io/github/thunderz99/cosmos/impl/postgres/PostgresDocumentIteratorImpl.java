package io.github.thunderz99.cosmos.impl.postgres;

import com.mongodb.client.MongoIterable;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosDocumentIterator;
import io.github.thunderz99.cosmos.CosmosDocumentList;
import io.github.thunderz99.cosmos.dto.MapIterator;
import io.github.thunderz99.cosmos.dto.TypedIterator;
import io.github.thunderz99.cosmos.util.Checker;
import org.bson.Document;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A postgres implementation of {@link CosmosDocumentIterator} interface, use to iterate {@link CosmosDocument} from an iterable of Map.
 *
 * <p>
 * {@link CosmosDocumentIterator} is an interface that represent an iterator of documents.
 * But the iterable is not a {@link Iterable<CosmosDocument>} but a {@link Iterable<Map>}.
 * So we need this class to convert the {@link Iterable<Map>} to a {@link Iterable<CosmosDocument>}
 * </p>
 */
public class PostgresDocumentIteratorImpl implements CosmosDocumentIterator {

    CosmosDocumentList docs;
    Iterator<? extends Map> iterator;

    PostgresDocumentIteratorImpl() {}

    PostgresDocumentIteratorImpl(CosmosDocumentList docs) {
        setDocumentIterable(docs);
    }

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

    @Override
    public <T> Iterator<T> getTypedIterator(Class<T> clazz) {
        Checker.checkNotNull(this.iterator, "this.iterator");
        Checker.checkNotNull(clazz, "clazz");
        return new TypedIterator(this, clazz);
    }

    @Override
    public MapIterator getMapIterator() {
        Checker.checkNotNull(this.iterator, "this.iterator");
        return new MapIterator(this);
    }

    /**
     * set the documents and the iterator will be reset to the beginning of the iterable
     * @param docs the docs to set
     */
    public void setDocumentIterable(CosmosDocumentList docs) {
        this.docs = docs;
        this.iterator = docs.toMap().iterator();
    }

    @Override
    public <T> T next(Class<T> clazz) {
        return next().toObject(clazz);
    }

    @Override
    public void close() {
        // todo
    }

}
