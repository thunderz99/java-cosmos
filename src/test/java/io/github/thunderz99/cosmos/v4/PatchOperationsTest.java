package io.github.thunderz99.cosmos.v4;

import com.azure.cosmos.implementation.RequestOptions;
import com.azure.cosmos.implementation.patch.PatchUtil;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PatchOperationsTest {

    @Test
    void cosmos_patch_serialization_should_preserve_nul_and_scalar_values() throws Exception {
        var value = "A\u0000B literal \\u0000\nline2\r\tend";
        var operations = PatchOperations.create().set("/value", TextNode.valueOf(value));

        var bytes = PatchUtil.serializeCosmosPatchToByteArray(
                operations.getCosmosPatchOperations(), new RequestOptions());
        var json = JsonUtil.getObjectMapper().readTree(bytes);

        assertThat(json.at("/operations/0/value").asText()).isEqualTo(value);
        assertThat(operations.size()).isEqualTo(1);
    }

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
                .add("/obj", new DummyPojo("Alice", 20))
                .set("/name", "Bob")
                .replace("/age", 21)
                .increment("/score", 1)
                .remove("/unused");

        var copied = operations.copy();

        assertThat(copied).isNotSameAs(operations);
        assertThat(copied.size()).isEqualTo(operations.size());
        assertThat(copied.getPatchOperations()).isNotSameAs(operations.getPatchOperations());
    }

    static class DummyPojo {
        String name;
        int age;

        DummyPojo(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

}
