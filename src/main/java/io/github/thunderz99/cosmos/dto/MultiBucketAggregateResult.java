package io.github.thunderz99.cosmos.dto;

/**
 * Aggregate result for one caller-defined condition bucket.
 */
public class MultiBucketAggregateResult extends RecordData {

    public String bucketId;

    /**
     * Number of records matching both the shared condition and the bucket condition.
     */
    public long matched;

    public Number value;

    public MultiBucketAggregateResult() {
    }

    public MultiBucketAggregateResult(String bucketId, long matched, Number value) {
        this.bucketId = bucketId;
        this.matched = matched;
        this.value = value;
    }
}
