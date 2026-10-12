package io.github.thunderz99.cosmos;

import com.azure.cosmos.implementation.patch.PatchOperationCore;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.thunderz99.cosmos.dto.BatchPatchOperation;
import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Persistence passes string contents through to the selected database unchanged.
 */
public final class PersistenceStringContractTestSupport {

    private PersistenceStringContractTestSupport() {
    }

    public static void assertStringsArePreservedAcrossPersistencePaths(
            CosmosDatabase db, String collection, String partition, String value) throws Exception {
        var prefix = "string-contract-" + UUID.randomUUID();
        var ids = new ArrayList<String>();

        try {
            var singleId = prefix + "-single";
            ids.add(singleId);
            var createData = document(singleId, value);
            createData.put("nested", Map.of("items", List.of(Map.of("text", value))));
            createData.put("literal", "literal \\u0000");
            createData.put("controls", "line1\nline2\r\tend");
            var originalData = JsonUtil.toMap(createData);

            var created = db.create(collection, createData, partition).toMap();
            assertThat(created).containsEntry("value", value)
                    .containsEntry("literal", "literal \\u0000")
                    .containsEntry("controls", "line1\nline2\r\tend")
                    .containsEntry("nested", Map.of("items", List.of(Map.of("text", value))));
            assertThat(createData).isEqualTo(originalData);

            assertThat(db.update(collection, document(singleId, value + "-update"), partition).toMap())
                    .containsEntry("value", value + "-update");
            assertThat(db.upsert(collection, document(singleId, value + "-upsert"), partition).toMap())
                    .containsEntry("value", value + "-upsert");
            assertThat(db.updatePartial(collection, singleId, Map.of("value", value + "-partial"), partition).toMap())
                    .containsEntry("value", value + "-partial");

            var operations = PatchOperations.create().set("/value", value + "-patch");
            assertThat(db.patch(collection, singleId, operations, partition).toMap())
                    .containsEntry("value", value + "-patch");
            assertThat(operations.size()).isEqualTo(1);
            assertThat(((PatchOperationCore<?>) operations.getPatchOperations().get(0)).getResource())
                    .isEqualTo(value + "-patch");

            // Keep the scalar compatibility fix independently of NUL sanitization.
            assertThat(db.patch(collection, singleId,
                    PatchOperations.create().set("/scalar", TextNode.valueOf(value)), partition).toMap())
                    .containsEntry("scalar", value);

            var batchIds = List.of(prefix + "-batch-1", prefix + "-batch-2");
            ids.addAll(batchIds);
            db.batchCreate(collection, batchIds.stream().map(id -> document(id, value)).toList(), partition);
            assertValues(db, collection, partition, batchIds, value);
            db.batchUpsert(collection, batchIds.stream().map(id -> document(id, value + "-batch-upsert")).toList(), partition);
            assertValues(db, collection, partition, batchIds, value + "-batch-upsert");

            var batchOperations = PatchOperations.create().set("/value", value + "-batch-patch");
            db.batchPatch(collection, batchIds.stream()
                    .map(id -> BatchPatchOperation.of(id, batchOperations)).toList(), partition);
            assertValues(db, collection, partition, batchIds, value + "-batch-patch");
            assertThat(batchOperations.size()).isEqualTo(1);

            var bulkIds = List.of(prefix + "-bulk-1", prefix + "-bulk-2");
            ids.addAll(bulkIds);
            db.bulkCreate(collection, bulkIds.stream().map(id -> document(id, value)).toList(), partition);
            assertValues(db, collection, partition, bulkIds, value);
            db.bulkUpsert(collection, bulkIds.stream().map(id -> document(id, value + "-bulk-upsert")).toList(), partition);
            assertValues(db, collection, partition, bulkIds, value + "-bulk-upsert");

            var bulkOperations = PatchOperations.create().set("/value", value + "-bulk-patch-same");
            db.bulkPatch(collection, bulkIds, bulkOperations, partition);
            assertValues(db, collection, partition, bulkIds, value + "-bulk-patch-same");
            assertThat(bulkOperations.size()).isEqualTo(1);
            var itemOperations = PatchOperations.create().set("/value", value + "-bulk-patch-item");
            db.bulkPatch(collection, bulkIds.stream()
                    .map(id -> BulkPatchOperation.of(id, itemOperations)).toList(), partition);
            assertValues(db, collection, partition, bulkIds, value + "-bulk-patch-item");
            assertThat(itemOperations.size()).isEqualTo(1);
        } finally {
            for (var id : ids) {
                try {
                    db.delete(collection, id, partition);
                } catch (Exception ignored) {
                    // Cleanup must not hide the failed assertion.
                }
            }
        }
    }

    private static LinkedHashMap<String, Object> document(String id, String value) {
        var document = new LinkedHashMap<String, Object>();
        document.put("id", id);
        document.put("value", value);
        return document;
    }

    private static void assertValues(CosmosDatabase db, String collection, String partition,
                                     List<String> ids, String expected) throws Exception {
        for (var id : ids) {
            assertThat(db.read(collection, id, partition).toMap()).containsEntry("value", expected);
        }
    }
}
