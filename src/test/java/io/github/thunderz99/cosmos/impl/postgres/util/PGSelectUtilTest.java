package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Sets;
import io.github.thunderz99.cosmos.condition.Condition;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PGSelectUtilTest {

    @Test
    void filtered_join_with_fields_should_materialize_even_for_small_projections() {
        for (var count : List.of(1, 55)) {
            var fields = IntStream.range(0, count).mapToObj(i -> "sheetContents.form.item" + i)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            fields.add("rooms");
            var condition = Condition.filter("rooms.no", 1).join(Set.of("rooms"))
                    .returnAllSubArray(false).fields(fields.toArray(String[]::new))
                    .offset(2).limit(3);
            var query = PGConditionUtil.toQuerySpec("schema1", condition, "table1");

            assertThat(query.queryText).startsWith("WITH filtered_data AS MATERIALIZED (\n")
                    .contains("jsonb_set(", "\nFROM filtered_data")
                    .endsWith(" OFFSET 2 LIMIT 3");
            assertThat(query.params).hasSize(2);
        }
    }

    @Test
    void queries_without_filtered_join_projection_should_not_materialize() {
        var conditions = List.of(
                Condition.filter("rooms.no", 1).fields("rooms").returnAllSubArray(false),
                Condition.filter("rooms.no", 1).join(Set.of("rooms"))
                        .returnAllSubArray(true).fields("rooms"),
                Condition.filter("rooms.no", 1).join(Set.of("rooms"))
                        .returnAllSubArray(false).fields(),
                Condition.filter("id", "member1").join(Set.of("rooms"))
                        .returnAllSubArray(false).fields("rooms"));

        for (var condition : conditions) {
            var query = PGConditionUtil.toQuerySpec("schema1", condition, "table1");
            assertThat(query.queryText).doesNotContain("WITH filtered_data", "MATERIALIZED");
        }
    }

    @Test
    void fields_at_or_below_fifty_keys_should_keep_existing_sql() {
        for (var count : List.of(49, 50)) {
            var fields = IntStream.range(0, count).mapToObj(i -> "field" + i)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            var pairs = fields.stream().map(key -> "'" + key + "', data->'" + key + "'")
                    .collect(Collectors.joining(", "));
            assertThat(PGSelectUtil.generateSelectByFields(fields))
                    .isEqualTo("id,\njsonb_build_object(" + pairs + ") AS \"data\"\n");
        }
    }

    @Test
    void oversized_nested_object_should_be_parenthesized() {
        var fields = IntStream.range(0, 51).mapToObj(i -> "contents.form.field" + i)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        var firstGroup = IntStream.range(0, 50)
                .mapToObj(i -> "'field" + i + "', data->'contents'->'form'->'field" + i + "'")
                .collect(Collectors.joining(", "));

        assertThat(PGSelectUtil.generateSelectByFields(fields)).isEqualTo(
                "id,\njsonb_build_object('contents', jsonb_build_object('form', (jsonb_build_object("
                        + firstGroup + ") || jsonb_build_object('field50', data->'contents'->'form'->'field50'))))"
                        + " AS \"data\"\n");
    }

    @Test
    void join_projection_should_keep_large_any_parameter_names_distinct() {
        var candidates = IntStream.range(0, 75).mapToObj(i -> "org-" + i).collect(Collectors.toList());
        for (var key : List.of("rooms.no ARRAY_CONTAINS_ANY", "rooms.tags ARRAY_CONTAINS_ANY info.id")) {
            var condition = Condition.filter(key, candidates).join(Set.of("rooms")).returnAllSubArray(false);
            var query = PGConditionUtil.toQuerySpec("schema1", condition, "table1");
            var placeholders = Pattern.compile("@[A-Za-z0-9_]+").matcher(query.queryText).results()
                    .map(match -> match.group()).collect(Collectors.toList());

            assertThat(query.params).hasSize(150);
            assertThat(placeholders).containsExactlyInAnyOrderElementsOf(query.params.stream()
                    .map(parameter -> parameter.name).collect(Collectors.toList()));
            assertThat(query.queryText).doesNotContain(" OR ");
            assertThat(query.queryText.split("@> ANY", -1)).hasSize(3);
        }
    }

    @Test
    void join_projection_should_keep_empty_any_false_in_both_clauses() {
        var condition = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", List.of())
                .join(Set.of("rooms")).returnAllSubArray(false);
        var query = PGConditionUtil.toQuerySpec("schema1", condition, "table1");

        assertThat(query.queryText.split("1=0", -1)).hasSize(3);
        assertThat(query.queryText).doesNotContain("ANY(");
        assertThat(query.params).isEmpty();
    }


    @Test
    void generateSelect_should_work() {
        {
            // select with no field
            var cond = Condition.filter()
                    .fields();
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo("*");
        }

        {
            // select with null fields
            var cond = Condition.filter()
                    .fields(null);
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo("*");
        }
    }



    @Test
    void generateSelectByFields_should_work(){
        {
            // normal case 1
            var fields = Sets.newLinkedHashSet(List.of("id", "name"));
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'name', data->'name') AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

        {
            // normal case : complicated keys
            var fields = Sets.newLinkedHashSet(List.of("contents.sheet-1.name", "contents.sheet-2.address"));
            var expected = """
                    id,
                    jsonb_build_object('contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address'))) AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

        {
            // normal case: select with fields that overlaps(sheet-1 overlaps, and sheet-2 standalone)
            var cond = Condition.filter()
                    .fields("id", "contents.sheet-1.name", "contents.sheet-1.age", "contents.sheet-2.address");
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name', 'age', data->'contents'->'sheet-1'->'age'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address'))) AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo(expected);
        }

    }
    
}
