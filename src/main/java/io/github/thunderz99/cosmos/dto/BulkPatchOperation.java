package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.v4.PatchOperations;

/**
 * A DTO representing a single bulk patch operation for a target document.
 */
public class BulkPatchOperation extends RecordData {

    /**
     * Target document id.
     */
    public String id;

    /**
     * Patch operations to apply to the target document.
     */
    public PatchOperations operations;

    public BulkPatchOperation() {
    }

    /**
     * Create a bulk patch operation with target id and patch operations.
     *
     * @param id target document id
     * @param operations patch operations
     */
    public BulkPatchOperation(String id, PatchOperations operations) {
        this.id = id;
        this.operations = operations;
    }
}
