package io.github.thunderz99.cosmos.condition;

import io.github.thunderz99.cosmos.dto.RecordData;

/**
 * An opaque bucket identifier and the condition used to match that bucket.
 */
public class ConditionBucket extends RecordData {

    public String bucketId;

    public Condition condition;

    public ConditionBucket() {
    }

    public static ConditionBucket of(String bucketId, Condition condition) {
        var ret = new ConditionBucket();
        ret.bucketId = bucketId;
        ret.condition = condition;
        return ret;
    }
}
