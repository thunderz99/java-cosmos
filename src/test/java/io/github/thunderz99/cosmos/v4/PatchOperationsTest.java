package io.github.thunderz99.cosmos.v4;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PatchOperationsTest {

    @Test
    void PatchOperations_should_work() {

        var operations = PatchOperations.create();
        operations.add("/add", 1);
        operations.remove("/remove");
        operations.set("/set",3 );
        operations.replace("/replace", 4);
        operations.increment("/increment", 5);

        assertThat(operations.size()).isEqualTo(5);

    }

    @Test
    void checkPath_should_work() {

        var operations = PatchOperations.create();
        assertThatThrownBy(() -> operations.add("add", 1)).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });
        assertThatThrownBy(() -> operations.remove("add")).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });
        assertThatThrownBy(() -> operations.set("add", 1)).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });
        assertThatThrownBy(() -> operations.replace("add", 1)).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });
        assertThatThrownBy(() -> operations.increment("add", 1)).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });
        assertThatThrownBy(() -> operations.increment("add", 1.5)).isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
            assertThat(e.getMessage().contains("Json path(add) must start with /"));
        });


    }

    @Test
    void copy_should_work() {
        var operations = PatchOperations.create()
                .add("/add", 1)
                .remove("/remove")
                .set("/set", 3)
                .replace("/replace", 4)
                .increment("/increment", 5)
                .increment("/incrementDouble", 1.5);

        var copied = operations.copy();
        assertThat(copied).isNotSameAs(operations);
        assertThat(copied.size()).isEqualTo(operations.size());

        copied.add("/new", "value");
        assertThat(operations.size()).isEqualTo(6);
        assertThat(copied.size()).isEqualTo(7);
    }

}
