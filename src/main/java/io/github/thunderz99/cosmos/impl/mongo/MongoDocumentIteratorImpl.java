package io.github.thunderz99.cosmos.impl.mongo;

import com.azure.cosmos.util.CosmosPagedIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoIterable;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosDocumentIterator;
import io.github.thunderz99.cosmos.dto.MapIterator;
import io.github.thunderz99.cosmos.dto.TypedIterator;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.bson.Document;

import java.util.*;
import java.util.stream.Stream;

/**
 * A mongodb implementation of {@link CosmosDocumentIterator} interface, use to iterate {@link CosmosDocument} from an iterable of Map.
 *
 * <p>
 * {@link CosmosDocumentIterator} is an interface that represent an iterator of documents.
 * But the iterable is not a {@link Iterable<CosmosDocument>} but a {@link Iterable<Map>}.
 * So we need this class to convert the {@link Iterable<Map>} to a {@link Iterable<CosmosDocument>}
 * </p>
 */
public class MongoDocumentIteratorImpl implements CosmosDocumentIterator {

    MongoIterable<Document> iterable;
    Iterator<? extends Map> iterator;

    MongoDocumentIteratorImpl() {}

    MongoDocumentIteratorImpl(MongoIterable<Document> iterable) {
        setDocumentIterable(iterable);
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
        return MongoDatabaseImpl.getCosmosDocument(iterator.next());
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
     * set the document iterable and the iterator will be reset to the beginning of the iterable
     * @param iterable the iterable to set
     */
    public void setDocumentIterable(MongoIterable<Document> iterable) {
        this.iterable = iterable;
        this.iterator = iterable.iterator();
    }

    @Override
    public <T> T next(Class<T> clazz) {
        return next().toObject(clazz);
    }

    @Override
    public void close() {
        iterable.iterator().close();
    }

}
