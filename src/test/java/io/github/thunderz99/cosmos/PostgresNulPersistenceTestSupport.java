package io.github.thunderz99.cosmos;

import io.github.thunderz99.cosmos.dto.BatchPatchOperation;
import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * PostgreSQL rejects real NUL instead of silently changing incoming business data.
 */
public final class PostgresNulPersistenceTestSupport {

    private PostgresNulPersistenceTestSupport() {
    }

    public static void assertNulIsRejectedAcrossPersistencePaths(
            CosmosDatabase db, String collection, String partition) throws Exception {
        var prefix = "nul-rejection-" + UUID.randomUUID();
        var existingId = prefix + "-existing";
        var otherId = prefix + "-other";
        var missingId = prefix + "-missing";
        var cleanId = prefix + "-clean";
        var ids = List.of(existingId, otherId, missingId, cleanId);
        var nul = "A\u0000B";
        // Also exercise a NUL value nested in a list of objects.
        var invalidDocument = Map.of("id", missingId, "nested", List.of(Map.of("value", nul)));

        try {
            assertRejected(() -> db.create(collection, invalidDocument, partition));
            assertMissing(db, collection, partition, missingId);
            assertRejected(() -> db.upsert(collection, invalidDocument, partition));
            assertMissing(db, collection, partition, missingId);

            var original = db.create(collection, Map.of("id", existingId, "value", "original"), partition).toMap();
            var other = db.create(collection, Map.of("id", otherId, "value", "other"), partition).toMap();
            var invalidUpdate = Map.of("id", existingId, "value", nul);

            assertRejected(() -> db.update(collection, invalidUpdate, partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertRejected(() -> db.upsert(collection, invalidUpdate, partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertRejected(() -> db.updatePartial(collection, existingId,
                    Map.of("value", nul, "newField", "must-not-be-written"), partition));
            assertUnchanged(db, collection, partition, existingId, original);

            var operations = PatchOperations.create()
                    .set("/newField", "must-not-be-written")
                    .set("/value", nul);
            assertRejected(() -> db.patch(collection, existingId, operations, partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertThat(operations.size()).isEqualTo(2);

            // Transactional APIs must roll back valid items as well as the invalid item.
            assertRejected(() -> db.batchCreate(collection, List.of(
                    Map.of("id", cleanId, "value", "valid"), invalidDocument), partition));
            assertMissing(db, collection, partition, cleanId);
            assertMissing(db, collection, partition, missingId);

            assertRejected(() -> db.batchUpsert(collection, List.of(
                    Map.of("id", existingId, "value", "must-roll-back"), invalidDocument), partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertMissing(db, collection, partition, missingId);

            assertRejected(() -> db.batchPatch(collection, List.of(
                    BatchPatchOperation.of(existingId, PatchOperations.create().set("/value", "must-roll-back")),
                    BatchPatchOperation.of(otherId, PatchOperations.create().set("/value", nul))), partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertUnchanged(db, collection, partition, otherId, other);

            // Non-transactional bulk APIs report the rejected item through fatalList.
            assertBulkRejected(db.bulkCreate(collection, List.of(invalidDocument), partition));
            assertMissing(db, collection, partition, missingId);
            assertBulkRejected(db.bulkUpsert(collection, List.of(invalidUpdate), partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertBulkRejected(db.bulkPatch(collection, List.of(existingId), operations, partition));
            assertUnchanged(db, collection, partition, existingId, original);
            assertBulkRejected(db.bulkPatch(collection,
                    List.of(BulkPatchOperation.of(existingId, operations)), partition));
            assertUnchanged(db, collection, partition, existingId, original);
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

    private static void assertRejected(ThrowingCallable action) {
        var error = catchThrowable(action);
        assertThat(error).as("PostgreSQL must reject a real NUL").isNotNull();
        assertNulSqlState(error);
        if (error instanceof CosmosException cosmosException) {
            assertThat(cosmosException.getStatusCode()).isEqualTo(400);
        }
    }

    private static void assertBulkRejected(CosmosBulkResult result) {
        assertThat(result.successList).isEmpty();
        assertThat(result.retryList).isEmpty();
        // Bulk wrappers retain the existing per-item 500 error contract, without exposing SQLState as a cause.
        assertThat(result.fatalList).singleElement()
                .satisfies(error -> assertThat(error.getStatusCode()).isEqualTo(500));
    }

    private static void assertNulSqlState(Throwable error) {
        for (var cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sqlException && "22P05".equals(sqlException.getSQLState())) {
                return;
            }
        }
        throw new AssertionError("Expected SQLState 22P05 in the exception chain", error);
    }

    private static void assertMissing(CosmosDatabase db, String collection, String partition, String id) {
        assertThatThrownBy(() -> db.read(collection, id, partition))
                .isInstanceOfSatisfying(CosmosException.class,
                        error -> assertThat(error.getStatusCode()).isEqualTo(404));
    }

    private static void assertUnchanged(CosmosDatabase db, String collection, String partition,
                                        String id, Map<String, Object> expected) throws Exception {
        assertThat(db.read(collection, id, partition).toMap()).isEqualTo(expected);
    }
}
