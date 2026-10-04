package io.github.thunderz99.cosmos;

import com.azure.cosmos.implementation.patch.PatchOperationCore;
import io.github.thunderz99.cosmos.dto.BatchPatchOperation;
import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.v4.PatchOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared U+0000 persistence contract assertions for every database implementation.
 */
public final class NulSanitizationContractTestSupport {

    private NulSanitizationContractTestSupport() {
    }

    /**
     * Verify U+0000 sanitization across all persistence APIs that accept string values.
     */
    public static void assertNulIsRemovedAcrossPersistencePaths(
            CosmosDatabase db, String collection, String partition) throws Exception {
        var prefix = "nul-sanitizer-" + UUID.randomUUID();
        var ids = new ArrayList<String>();

        try {
            var singleId = prefix + "-single";
            ids.add(singleId);
            var createData = document(singleId, "value", "create\u0000value");
            createData.put("nested", Map.of("text", "nested\u0000value"));
            createData.put("literal", "literal \\u0000");
            createData.put("controls", "line1\nline2\r\tend");

            var created = db.create(collection, createData, partition).toMap();
            assertThat(created).containsEntry("value", "createvalue")
                    .containsEntry("literal", "literal \\u0000")
                    .containsEntry("controls", "line1\nline2\r\tend");
            assertThat(((Map<?, ?>) created.get("nested")).get("text")).isEqualTo("nestedvalue");

            var updated = db.update(
                    collection, document(singleId, "value", "update\u0000value"), partition).toMap();
            assertThat(updated).containsEntry("value", "updatevalue");

            var partial = db.updatePartial(
                    collection, singleId, Map.of("partial", "partial\u0000value"), partition).toMap();
            assertThat(partial).containsEntry("partial", "partialvalue");

            var patchOperations = PatchOperations.create().set("/patched", "patch\u0000value");
            var patched = db.patch(collection, singleId, patchOperations, partition).toMap();
            assertThat(patched).containsEntry("patched", "patchvalue");
            assertThat(((PatchOperationCore<?>) patchOperations.getPatchOperations().get(0)).getResource())
                    .isEqualTo("patch\u0000value");

            var batchIds = List.of(prefix + "-batch-1", prefix + "-batch-2");
            ids.addAll(batchIds);
            db.batchCreate(collection, batchIds.stream()
                    .map(id -> document(id, "value", "batch-create\u0000value"))
                    .toList(), partition);
            assertValues(db, collection, partition, batchIds, "batch-createvalue");

            db.batchUpsert(collection, batchIds.stream()
                    .map(id -> document(id, "value", "batch-upsert\u0000value"))
                    .toList(), partition);
            assertValues(db, collection, partition, batchIds, "batch-upsertvalue");

            db.batchPatch(collection, batchIds.stream()
                    .map(id -> BatchPatchOperation.of(
                            id, PatchOperations.create().set("/value", "batch-patch\u0000value")))
                    .toList(), partition);
            assertValues(db, collection, partition, batchIds, "batch-patchvalue");

            var bulkIds = List.of(prefix + "-bulk-1", prefix + "-bulk-2");
            ids.addAll(bulkIds);
            db.bulkCreate(collection, bulkIds.stream()
                    .map(id -> document(id, "value", "bulk-create\u0000value"))
                    .toList(), partition);
            assertValues(db, collection, partition, bulkIds, "bulk-createvalue");

            db.bulkUpsert(collection, bulkIds.stream()
                    .map(id -> document(id, "value", "bulk-upsert\u0000value"))
                    .toList(), partition);
            assertValues(db, collection, partition, bulkIds, "bulk-upsertvalue");

            db.bulkPatch(collection, bulkIds,
                    PatchOperations.create().set("/value", "bulk-patch-same\u0000value"), partition);
            assertValues(db, collection, partition, bulkIds, "bulk-patch-samevalue");

            db.bulkPatch(collection, bulkIds.stream()
                    .map(id -> BulkPatchOperation.of(
                            id, PatchOperations.create().set("/value", "bulk-patch\u0000value")))
                    .toList(), partition);
            assertValues(db, collection, partition, bulkIds, "bulk-patchvalue");
        } finally {
            for (var id : ids) {
                try {
                    db.delete(collection, id, partition);
                } catch (Exception ignored) {
                    // Best-effort cleanup must not hide the contract assertion failure.
                }
            }
        }
    }

    private static LinkedHashMap<String, Object> document(String id, String key, String value) {
        var document = new LinkedHashMap<String, Object>();
        document.put("id", id);
        document.put(key, value);
        return document;
    }

    private static void assertValues(CosmosDatabase db, String collection, String partition,
                                     List<String> ids, String expected) throws Exception {
        for (var id : ids) {
            assertThat(db.read(collection, id, partition).toMap()).containsEntry("value", expected);
        }
    }
}
