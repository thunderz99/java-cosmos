package io.github.thunderz99.cosmos.impl.cosmosdb;

import com.azure.cosmos.util.CosmosPagedIterable;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosDocumentIterator;
import io.github.thunderz99.cosmos.dto.MapIterator;
import io.github.thunderz99.cosmos.dto.TypedIterator;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * An implementation of {@link CosmosDocumentIterator} interface, use to iterate {@link CosmosDocument} from an iterable of Map.
 *
 * <p>
 * {@link CosmosDocumentIterator} is an interface that represent an iterator of documents.
 * But the iterable is not a {@link Iterable<CosmosDocument>} but a {@link Iterable<Map>}.
 * So we need this class to convert the {@link Iterable<Map>} to a {@link Iterable<CosmosDocument>}
 * </p>
 */
public class CosmosDocumentIteratorImpl implements CosmosDocumentIterator {

    CosmosPagedIterable<? extends Map> iterable;
    Iterator<? extends Map> iterator;

    /**
     * if keyMap is not null, this is a query using join on sub array. and the result should replace subArray to subArray only match the join.
     */
    Map<String, String[]> keyMap = null;

    CosmosDocumentIteratorImpl() {}

    CosmosDocumentIteratorImpl(CosmosPagedIterable<? extends Map> iterable) {
        this.iterable = iterable;
        this.iterator = this.iterable.iterator();
    }

    CosmosDocumentIteratorImpl(CosmosPagedIterable<? extends Map> iterable, Map<String, String[]> keyMap) {
        this.iterable = iterable;
        this.iterator = this.iterable.iterator();
        this.keyMap = keyMap;
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

        Map<String, Object> objMap = null;
        if(keyMap != null){
            objMap = joinOnSubArray(iterator.next(), keyMap);
        } else {
            objMap = iterator.next();
        }

        return new CosmosDocument(objMap);
    }

    /**
     * set the document iterable and the iterator will be reset to the beginning of the iterable
     * @param iterable the iterable to set
     */
    public void setDocumentIterable(CosmosPagedIterable<? extends Map> iterable) {
        this.iterable = iterable;
        this.iterator = iterable.iterator();
    }

    @Override
    public <T> T next(Class<T> clazz) {
        return next().toObject(clazz);
    }

    @Override
    public <T> Iterator<T> getTypedIterator(Class<T> clazz) {
        return new TypedIterator(this, clazz);
    }

    @Override
    public MapIterator getMapIterator() {
        Checker.checkNotNull(this.iterator, "this.iterator");
        return new MapIterator(this);
    }

    /**
     * Return a stream of docs in the iterable. When using join(and returnAllSubArray is false), the docs will be replaced by subArray only match the join
     * @return a stream of docs in the iterable
     */
    public Stream<? extends Map> stream() {

        if(keyMap == null) {
            // normal query
            return this.iterable.stream();
        }

        //query with join and returnAllSubArray = false
        return this.iterable.stream().map(doc -> joinOnSubArray(doc, keyMap));
    }

    /**
     * Traverse the result of join part and replaced by new result that is found by sub query.
     * This method is used in the join method to traverse the result of join part and replaced by new result that is found by sub query.
     * @param doc the document
     * @param keyMap join part map
     * @return the merged sub array
     */
    static Map<String, Object> joinOnSubArray(Map<String, Object> doc, Map<String, String[]> keyMap) {
        var docMain = JsonUtil.toMap(doc.get("c"));

        for (Map.Entry<String, String[]> entry : keyMap.entrySet()) {
            if (Objects.nonNull(doc.get(entry.getKey()))) {
                Map<String, Object> docSubListItem = Map.of(entry.getKey(), doc.get(entry.getKey()));
                traverseListValueToDoc(docMain, docSubListItem, entry, 0);
            }
        }
        return docMain;
    }

    /**
     * Traverse and merge the content of the list to origin list
     * This function will traverse the result of join part and replaced by new result that is found by sub query.
     *
     * @param docMap    the map of doc
     * @param newSubMap new sub map
     * @param entry     entry
     * @param count     count
     */
    static void traverseListValueToDoc(Map<String, Object> docMap, Map<String, Object> newSubMap, Map.Entry<String, String[]> entry, int count) {

        var aliasName = entry.getKey();
        var subValue = entry.getValue();

        if (count == subValue.length - 1) {
            if (newSubMap.get(aliasName) instanceof List) {
                docMap.put(subValue[entry.getValue().length - 1], newSubMap.get(aliasName));
            }
            return;
        }

        if (docMap.get(subValue[count]) instanceof Map) {
            traverseListValueToDoc((Map) docMap.get(subValue[count++]), newSubMap, entry, count);
        }
    }
}
