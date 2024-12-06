package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;

import io.github.thunderz99.cosmos.dto.CheckBox;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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


    @Test
    void toPostgresPatchData_should_work() {
        var operations = PatchOperations.create()
                .set("/age", 20) // set
                .add("/fullName/first","Tom") // add
                .remove("/address/street") // remove
        ;

        var result = JsonPatchUtil.toPostgresPatchData(operations);

        assertThat(result.getQueryText()).isEqualTo("jsonb_set( jsonb_set( jsonb_set( jsonb_set( data,'{age}', @param000_age::jsonb ),'{fullName}', COALESCE(data #>'{fullName}','{}'::jsonb), true),'{fullName,first}', @param001_fullName__first::jsonb ),'{address}', (data #> '{address}') - 'street' )");
        assertThat(result.getParameters()).hasSize(2);
        assertThat(result.getParameters().get(0).getName()).isEqualTo("@param000_age");
        assertThat(result.getParameters().get(0).getValue()).isEqualTo(20);
        assertThat(result.getParameters().get(1).getName()).isEqualTo("@param001_fullName__first");
        assertThat(result.getParameters().get(1).getValue()).isEqualTo("Tom");
    }

    @Test
    void generateSubSql4NestedJson_should_work() {
        {   // normal case, to patch "/address"
            var subSql = JsonPatchUtil.generateSubSql4NestedJson("data", List.of("address"), 1, "@param001_address");
            assertThat(subSql).isEqualTo("jsonb_set( data,'{address}', @param001_address::jsonb )");
        }

        {   // normal case, to patch "/address/city"
            var subSql = JsonPatchUtil.generateSubSql4NestedJson("data", List.of("address", "city"), 2, "@param001_address__city");
            assertThat(subSql).isEqualTo("jsonb_set( jsonb_set( data,'{address}', COALESCE(data #>'{address}','{}'::jsonb), true),'{address,city}', @param001_address__city::jsonb )");
        }

        {   // normal case, to patch "/address/city/street"
            var subSql = JsonPatchUtil.generateSubSql4NestedJson("data", List.of("address", "city", "street"), 3, "@param001_address__city__street");
            assertThat(subSql).isEqualTo("jsonb_set( jsonb_set( jsonb_set( data,'{address}', COALESCE(data #>'{address}','{}'::jsonb), true),'{address,city}', COALESCE(data #>'{address,city}','{}'::jsonb), true),'{address,city,street}', @param001_address__city__street::jsonb )");
        }

        {   // irregular case, keys is empty
            var subSql = JsonPatchUtil.generateSubSql4NestedJson("data", List.of(), 0, "@param001");
            assertThat(subSql).isEqualTo("data");
        }

        {   // irregular case, keys contains blank string
            assertThatThrownBy( () -> JsonPatchUtil.generateSubSql4NestedJson("data", List.of("address", " "), 2, "@param001"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key[1] should be non-blank");
        }

        {   // irregular case, paramName is empty
            assertThatThrownBy( () -> JsonPatchUtil.generateSubSql4NestedJson("data", List.of("address", "city"), 2, ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("paramName should be non-blank");
        }
    }

    @Test
    void generateSubSql4Remove_should_work(){
        {   // normal case, to remove "/address"
            var subSql = JsonPatchUtil.generateSubSql4Remove("data", List.of("address"));
            assertThat(subSql).isEqualTo("data - 'address'");
        }

        {   // normal case, to remove "/address/city"
            var subSql = JsonPatchUtil.generateSubSql4Remove("data", List.of("address", "city"));
            assertThat(subSql).isEqualTo("jsonb_set( data,'{address}', (data #> '{address}') - 'city' )");
        }

        {   // normal case, to remove "/address/city/street"
            var subSql = JsonPatchUtil.generateSubSql4Remove("data", List.of("address", "city", "street"));
            assertThat(subSql).isEqualTo("jsonb_set( data,'{address,city}', (data #> '{address,city}') - 'street' )");
        }

        {   // normal case, to remove "/contents/skills/2"
            var subSql = JsonPatchUtil.generateSubSql4Remove("data", List.of("contents", "skills", "2"));
            assertThat(subSql).isEqualTo("jsonb_set( data,'{contents,skills}', (data #> '{contents,skills}') - 2 )");
        }


        {   // irregular case, keys is empty
            var subSql = JsonPatchUtil.generateSubSql4Remove("data", List.of());
            assertThat(subSql).isEqualTo("data");
        }

        {   // irregular case, keys contains blank string
            assertThatThrownBy( () -> JsonPatchUtil.generateSubSql4Remove("data", List.of("address", " ")) )
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key[1] should be non-blank");
        }
    }

    @Test
    void toPostgresPath_should_work() {

        {   // normal case, to patch "/address"
            var path = JsonPatchUtil.toPostgresPath("/address");
            assertThat(path).isEqualTo("{address}");
        }

        {   // normal case, to patch "/address/city"
            var path = JsonPatchUtil.toPostgresPath("/address/city");
            assertThat(path).isEqualTo("{address,city}");
        }

        {   // normal case, to patch "/address/city/street"
            var path = JsonPatchUtil.toPostgresPath("/address/city/street");
            assertThat(path).isEqualTo("{address,city,street}");
        }

        {   // irregular case, path is empty
            assertThat(JsonPatchUtil.toPostgresPath("")).isEqualTo("");
            assertThat(JsonPatchUtil.toPostgresPath(null)).isEqualTo(null);
        }

        {   // irregular case, path is invalid
            assertThatThrownBy( () -> JsonPatchUtil.toPostgresPath("invalid path contains ; select *") )
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("path should not contain semicolon ';'");
        }
    }

    @Test
    void generateAddOperation4ArrayPostgres_should_work(){
        {   // normal case, to ADD at "/skills/1"
            var subSql = JsonPatchUtil.generateAddOperation4ArrayPostgres("data", List.of("skills", "1"), "@param001_skills__1");
            assertThat(subSql).isEqualTo("jsonb_set( data,'{skills}', jsonb_insert( data #> '{skills}', '{1}', @param001_skills__1::jsonb) )");
        }

        {   // normal case, to ADD at "/contents/IT/skills/2"
            var subSql = JsonPatchUtil.generateAddOperation4ArrayPostgres("data", List.of("contents", "IT", "skills", "2"), "@param001_skills__2");
            assertThat(subSql).isEqualTo("jsonb_set( data,'{contents,IT,skills}', jsonb_insert( data #> '{contents,IT,skills}', '{2}', @param001_skills__2::jsonb) )");
        }

        {   // irregular case, keys is empty
            var subSql = JsonPatchUtil.generateAddOperation4ArrayPostgres("data", List.of(), "@param000");
            assertThat(subSql).isEqualTo("data");
        }

        {   // irregular case, keys' size is only 1
            assertThatThrownBy( () -> JsonPatchUtil.generateAddOperation4ArrayPostgres("data", List.of("1"), "@param001_skills__") )
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ADD operation for array should have at least 2 keys(e.g. [\"skills\", 1]). given:[1]");
        }

        {   // irregular case, keys contains blank string
            assertThatThrownBy( () -> JsonPatchUtil.generateAddOperation4ArrayPostgres("data", List.of("skills", ""), "@param001_skills__") )
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key[1] should be non-blank");
        }

    }
}