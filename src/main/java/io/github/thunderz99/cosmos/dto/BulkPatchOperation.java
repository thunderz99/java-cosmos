package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.v4.PatchOperations;

/**
 * A patch task used by bulkPatch.
 */
public class BulkPatchOperation {

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
    public BulkPatchOperation() {
    }

    /**
     * Create an operation with id and patch operations.
     *
     * @param id         target document id
     * @param operations patch operations
     */
    public BulkPatchOperation(String id, PatchOperations operations) {
        this.id = id;
        this.operations = operations;
    }

    /**
     * Build a bulk patch operation.
     *
     * @param id         target document id
     * @param operations patch operations
     * @return BulkPatchOperation
     */
    public static BulkPatchOperation of(String id, PatchOperations operations) {
        return new BulkPatchOperation(id, operations);
    }
}
