package io.github.thunderz99.cosmos.util;

/**
 * Shared limits for batch/chunk style operations.
 */
public final class CosmosLimits {

    private CosmosLimits() {
    }

    /**
     * Maximum operations allowed in one batch request.
     */
    public static final int BATCH_OPERATION_LIMIT = 100;

    /**
     * Default chunk size used by bulk operations.
     */
    public static final int BULK_CHUNK_SIZE = BATCH_OPERATION_LIMIT;
}
