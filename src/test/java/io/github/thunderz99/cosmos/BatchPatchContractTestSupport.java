package io.github.thunderz99.cosmos;

import io.github.thunderz99.cosmos.dto.BatchPatchOperation;
import io.github.thunderz99.cosmos.v4.PatchOperations;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Shared batchPatch contract assertions used by every database integration test.
 */
public final class BatchPatchContractTestSupport {

    private BatchPatchContractTestSupport() {
    }

    public static void assertMultipleDocumentsArePatched(CosmosDatabase db, String coll, String partition) throws Exception {
        var documents = createDocuments("multiple");

        try {
            db.batchCreate(coll, documents, partition);
            var operations = documents.stream()
                    .map(document -> BatchPatchOperation.of(
                            document.id,
                            PatchOperations.create().set("/name", "patched")))
                    .toList();

            var result = db.batchPatch(coll, operations, partition);

            assertThat(result).hasSize(documents.size());
            assertThat(result)
                    .extracting(document -> document.toObject(BatchPatchDocument.class).name)
                    .containsExactly("patched", "patched");
            assertThat(readNames(db, coll, partition, documents)).containsExactly("patched", "patched");
        } finally {
            db.batchDelete(coll, documents, partition);
        }
    }

    public static void assertDifferentOperationsAreApplied(CosmosDatabase db, String coll, String partition) throws Exception {
        var documents = createDocuments("different");

        try {
            db.batchCreate(coll, documents, partition);
            var operations = List.of(
                    BatchPatchOperation.of(documents.get(0).id,
                            PatchOperations.create().set("/name", "first-patched").increment("/version", 1)),
                    BatchPatchOperation.of(documents.get(1).id,
                            PatchOperations.create().set("/name", "second-patched").increment("/version", 2)));

            var result = db.batchPatch(coll, operations, partition).stream()
                    .map(document -> document.toObject(BatchPatchDocument.class))
                    .toList();

            assertThat(result).extracting(document -> document.name)
                    .containsExactly("first-patched", "second-patched");
            assertThat(result).extracting(document -> document.version)
                    .containsExactly(2, 3);
        } finally {
            db.batchDelete(coll, documents, partition);
        }
    }

    public static void assertAllUpdatesAreRolledBack(CosmosDatabase db, String coll, String partition) throws Exception {
        var documents = createDocuments("rollback");
        var missingId = "batch_patch_missing_" + UUID.randomUUID();

        try {
            db.batchCreate(coll, documents, partition);
            var operations = List.of(
                    BatchPatchOperation.of(documents.get(0).id, PatchOperations.create().set("/name", "should-rollback-1")),
                    BatchPatchOperation.of(missingId, PatchOperations.create().set("/name", "missing")),
                    BatchPatchOperation.of(documents.get(1).id, PatchOperations.create().set("/name", "should-rollback-2")));

            assertThatThrownBy(() -> db.batchPatch(coll, operations, partition))
                    .isInstanceOf(CosmosException.class);

            assertThat(readNames(db, coll, partition, documents)).containsExactly("before-1", "before-2");
        } finally {
            db.batchDelete(coll, documents, partition);
        }
    }

    public static void assertMoreThanOneHundredDocumentsAreRejected(CosmosDatabase db, String coll, String partition) {
        var operations = IntStream.rangeClosed(0, 100)
                .mapToObj(index -> BatchPatchOperation.of(
                        "batch_patch_limit_" + index,
                        PatchOperations.create().set("/name", "patched")))
                .toList();

        assertThatThrownBy(() -> db.batchPatch(coll, operations, partition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should not exceed 100");
    }

    public static void assertMoreThanTenPatchOperationsAreRejected(CosmosDatabase db, String coll, String partition) {
        var patchOperations = PatchOperations.create();
        IntStream.rangeClosed(1, 11)
                .forEach(index -> patchOperations.set("/field" + index, index));
        var operations = List.of(BatchPatchOperation.of("batch_patch_operation_limit", patchOperations));

        assertThatThrownBy(() -> db.batchPatch(coll, operations, partition))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("less or equal to 10");
    }

    private static List<BatchPatchDocument> createDocuments(String scenario) {
        var suffix = UUID.randomUUID().toString();
        return List.of(
                new BatchPatchDocument("batch_patch_" + scenario + "_1_" + suffix, "before-1", 1),
                new BatchPatchDocument("batch_patch_" + scenario + "_2_" + suffix, "before-2", 1));
    }

    private static List<String> readNames(CosmosDatabase db, String coll, String partition,
                                          List<BatchPatchDocument> documents) throws Exception {
        return documents.stream()
                .map(document -> readName(db, coll, partition, document.id))
                .toList();
    }

    private static String readName(CosmosDatabase db, String coll, String partition, String id) {
        try {
            return db.read(coll, id, partition).toObject(BatchPatchDocument.class).name;
        } catch (Exception e) {
            throw new AssertionError("Failed to read batchPatch test document: " + id, e);
        }
    }

    public static class BatchPatchDocument {
        public String id;
        public String name;
        public int version;

        public BatchPatchDocument() {
        }

        public BatchPatchDocument(String id, String name, int version) {
            this.id = id;
            this.name = name;
            this.version = version;
        }
    }
}
