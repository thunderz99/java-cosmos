package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;

import io.github.thunderz99.cosmos.dto.CheckBox;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;

class JsonPatchUtilTest {

    @Test
    void getNormalizedValue_should_work() {

        assertThat(JsonPatchUtil.getNormalizedValue(null)).isNull();

        assertThat(JsonPatchUtil.getNormalizedValue(1)).isEqualTo(1);
        assertThat(JsonPatchUtil.getNormalizedValue("str")).isEqualTo("str");

        assertThat(JsonPatchUtil.getNormalizedValue(Map.of("id", "id1")))
                .asInstanceOf(MAP).containsEntry("id", "id1");

        assertThat(JsonPatchUtil.getNormalizedValue(List.of("id1", "id2")))
                .asInstanceOf(LIST).contains("id1", "id2");

        assertThat(JsonPatchUtil.getNormalizedValue(new CheckBox("id1", "name1", CheckBox.Align.VERTICAL)))
                .asInstanceOf(MAP)
                .containsEntry("id", "id1")
                .containsEntry("name", "name1")
                .containsEntry("align", "VERTICAL");
        ;

        assertThat(JsonPatchUtil.getNormalizedValue(List.of(
                        new CheckBox("id1", "name1", CheckBox.Align.VERTICAL),
                        new CheckBox("id2", "name2", CheckBox.Align.HORIZONTAL)
                        )
                ))
                .isInstanceOfSatisfying(List.class, list -> {
                    assertThat((Map<String, Object>) list.get(0))
                            .containsEntry("id", "id1")
                            .containsEntry("name", "name1")
                            .containsEntry("align", "VERTICAL");
                    assertThat((Map<String, Object>) list.get(1))
                            .containsEntry("id", "id2")
                            .containsEntry("name", "name2")
                            .containsEntry("align", "HORIZONTAL");

                });

        assertThat(JsonPatchUtil.getNormalizedValue(Map.of("check1", new CheckBox("id1", "name1", CheckBox.Align.VERTICAL))))
                .isInstanceOfSatisfying(Map.class, map -> {
                    assertThat(map.get("check1")).isInstanceOf(Map.class);
                    assertThat((Map<String, Object>) map.get("check1"))
                            .containsEntry("name", "name1")
                            .containsEntry("align", "VERTICAL");
                })

        ;

    }
}