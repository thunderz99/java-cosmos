package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.v4.PatchOperations;

/**
 * A document patch task used by batchPatch.
 */
public class BatchPatchOperation {

    /**
     * Target document id.
     */
    public String id;

    /**
     * Patch operations for the target document.
     */
    public PatchOperations operations;

    /**
     * Create an empty operation.
     */
    public BatchPatchOperation() {
    }

    /**
     * Create an operation with id and patch operations.
     *
     * @param id         target document id
     * @param operations patch operations
     */
    public BatchPatchOperation(String id, PatchOperations operations) {
        this.id = id;
        this.operations = operations;
    }

    /**
     * Build a batch patch operation.
     *
     * @param id         target document id
     * @param operations patch operations
     * @return BatchPatchOperation
     */
    public static BatchPatchOperation of(String id, PatchOperations operations) {
        return new BatchPatchOperation(id, operations);
    }
}
