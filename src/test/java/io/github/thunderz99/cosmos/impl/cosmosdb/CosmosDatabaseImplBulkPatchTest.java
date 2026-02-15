package io.github.thunderz99.cosmos.impl.cosmosdb;

import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CosmosDatabaseImplBulkPatchTest {

    @Test
    void doCheckBeforeBulkPatch_should_work() {
        var operation = BulkPatchOperation.of("bulk_patch_id_001", PatchOperations.create().set("/name", "Alice"));

        assertThatCode(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .doesNotThrowAnyException();
    }

    @Test
    void doCheckBeforeBulkPatch_should_work_for_boundary_max_operations() {
        var patchOperations = PatchOperations.create()
                .set("/k1", 1)
                .set("/k2", 2)
                .set("/k3", 3)
                .set("/k4", 4)
                .set("/k5", 5)
                .set("/k6", 6)
                .set("/k7", 7)
                .set("/k8", 8)
                .set("/k9", 9)
                .set("/k10", 10);

        var operation = BulkPatchOperation.of("bulk_patch_id_002", patchOperations);

        assertThatCode(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .doesNotThrowAnyException();
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_coll_is_blank() {
        var operation = BulkPatchOperation.of("bulk_patch_id_003", PatchOperations.create().set("/name", "Bob"));

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("", List.of(operation), "Users"))
                .hasMessageContaining("coll should be non-blank");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_partition_is_blank() {
        var operation = BulkPatchOperation.of("bulk_patch_id_004", PatchOperations.create().set("/name", "Bob"));

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), ""))
                .hasMessageContaining("partition should be non-blank");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_data_is_empty_or_null() {
        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(), "Users"))
                .hasMessageContaining("should not be empty collection");
        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", null, "Users"))
                .hasMessageContaining("should not be empty collection");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_operation_is_null() {
        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", Arrays.asList((BulkPatchOperation) null), "Users"))
                .hasMessageContaining("bulkPatch operation should not be null");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_id_is_blank() {
        var operation = BulkPatchOperation.of("", PatchOperations.create().set("/name", "Carl"));

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("bulkPatch operation id should be non-blank");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_id_is_invalid() {
        var operation = BulkPatchOperation.of("invalid_id\n", PatchOperations.create().set("/name", "Dave"));

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("id cannot contain \\t or \\n or \\r or /");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_patch_operations_is_null() {
        var operation = BulkPatchOperation.of("bulk_patch_id_005", null);

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("bulkPatch operation patch operations should not be null");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_patch_operations_exceed_limit() {
        var patchOperations = PatchOperations.create()
                .set("/k1", 1)
                .set("/k2", 2)
                .set("/k3", 3)
                .set("/k4", 4)
                .set("/k5", 5)
                .set("/k6", 6)
                .set("/k7", 7)
                .set("/k8", 8)
                .set("/k9", 9)
                .set("/k10", 10)
                .set("/k11", 11);

        var operation = BulkPatchOperation.of("bulk_patch_id_006", patchOperations);

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("Size of operations should be less or equal to 10");
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_work() {
        var ids = List.of("bulk_patch_id_101", "bulk_patch_id_102");
        var operations = PatchOperations.create().set("/name", "Eve");

        assertThatCode(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", ids, operations, "Users"))
                .doesNotThrowAnyException();
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_throw_when_ids_empty() {
        var operations = PatchOperations.create().set("/name", "Eve");

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(), operations, "Users"))
                .hasMessageContaining("should not be empty collection");
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_throw_when_operations_null() {
        var ids = List.of("bulk_patch_id_103");

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", ids, null, "Users"))
                .hasMessageContaining("bulkPatch operations should not be null");
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_throw_when_operations_exceed_limit() {
        var ids = List.of("bulk_patch_id_104");
        var operations = PatchOperations.create()
                .set("/k1", 1)
                .set("/k2", 2)
                .set("/k3", 3)
                .set("/k4", 4)
                .set("/k5", 5)
                .set("/k6", 6)
                .set("/k7", 7)
                .set("/k8", 8)
                .set("/k9", 9)
                .set("/k10", 10)
                .set("/k11", 11);

        assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulkPatch("Users", ids, operations, "Users"))
                .hasMessageContaining("Size of operations should be less or equal to 10");
    }
}
