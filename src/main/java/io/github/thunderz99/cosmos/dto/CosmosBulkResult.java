package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosException;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of bulk operations
 */
public class CosmosBulkResult {

    /**
     * The result of success operations
     */
    public List<CosmosDocument> successList = new ArrayList<>();

    /**
     * The result of retries operations that exceed the max retry times
     */
    public List<?> retryList = new ArrayList<>();

    /**
     * The result of fatal operations. e.g. 409 Conflict
     */
    public List<CosmosException> fatalList = new ArrayList<>();
}
