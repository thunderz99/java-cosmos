package io.github.thunderz99.cosmos.impl.postgres;

import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresDatabaseImplBulkPatchTest {

    @Test
    void doCheckBeforeBulkPatch_should_work() {
        var operation = BulkPatchOperation.of("bulk_patch_id_001", PatchOperations.create().set("/name", "Alice"));

        assertThatCode(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
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

        assertThatCode(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .doesNotThrowAnyException();
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_data_is_empty_or_null() {
        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(), "Users"))
                .hasMessageContaining("should not be empty collection");
        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", null, "Users"))
                .hasMessageContaining("should not be empty collection");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_operation_is_null() {
        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", Arrays.asList((BulkPatchOperation) null), "Users"))
                .hasMessageContaining("bulkPatch operation should not be null");
    }

    @Test
    void doCheckBeforeBulkPatch_should_throw_when_id_is_invalid() {
        var operation = BulkPatchOperation.of("invalid_id\n", PatchOperations.create().set("/name", "Dave"));

        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("id cannot contain \\t or \\n or \\r or /");
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

        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", List.of(operation), "Users"))
                .hasMessageContaining("Size of operations should be less or equal to 10");
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_work() {
        var ids = List.of("bulk_patch_id_101", "bulk_patch_id_102");
        var operations = PatchOperations.create().set("/name", "Eve");

        assertThatCode(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", ids, operations, "Users"))
                .doesNotThrowAnyException();
    }

    @Test
    void doCheckBeforeBulkPatch_with_ids_should_throw_when_operations_null() {
        var ids = List.of("bulk_patch_id_103");

        assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulkPatch("Users", ids, null, "Users"))
                .hasMessageContaining("bulkPatch operations should not be null");
    }
}
