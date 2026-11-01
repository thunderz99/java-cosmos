package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.SubQueryExpression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PGSubQueryExpressionTest {

    @Test
    void array_contains_any_should_reference_the_field_once_for_mixed_values() {
        var expression = new PGSubQueryExpression("assignedOrgIds", "", List.of(3, "3", "org-1"),
                PGSubQueryExpression.ARRAY_CONTAINS_ANY);
        var query = expression.toQuerySpec(new AtomicInteger(), "data");

        assertThat(query.queryText).isEqualTo(" (data->'assignedOrgIds' @> ANY(ARRAY["
                + "@param000_assignedOrgIds__0::jsonb, @param000_assignedOrgIds__1::jsonb, "
                + "@param000_assignedOrgIds__2::jsonb]::jsonb[]))");
        assertThat(query.params).extracting(parameter -> parameter.value)
                .containsExactly("3", "\"3\"", "\"org-1\"");
    }

    @Test
    void array_contains_any_should_preserve_legacy_non_integer_conversion_and_escape_strings() {
        var value = "quote\" and slash\\ and apostrophe'";
        var params = new ArrayList<CosmosSqlParameter>();
        PGSubQueryExpression.buildArrayContainsAny("items", "", "@items", List.of(value, 3L, true),
                params, "data");

        assertThat(params).extracting(parameter -> parameter.value)
                .containsExactly(JsonUtil.toJson(value), "\"3\"", "\"true\"");
    }

    @Test
    void array_contains_any_should_build_typed_nested_candidates_with_a_join_alias() {
        var params = new ArrayList<CosmosSqlParameter>();
        var query = PGSubQueryExpression.buildArrayContainsAny("rooms", "info.id", "@rooms_info_id",
                List.of(3, "3"), params, "j0");

        assertThat(query).isEqualTo(" (j0->'rooms' @> ANY(ARRAY["
                + "jsonb_build_array(jsonb_build_object('info', jsonb_build_object('id', @rooms_info_id__0))), "
                + "jsonb_build_array(jsonb_build_object('info', jsonb_build_object('id', @rooms_info_id__1)))"
                + "]::jsonb[]))");
        assertThat(params).extracting(parameter -> parameter.value).containsExactly(3, "3");
    }

    @Test
    void array_contains_any_should_keep_empty_lists_false_without_parameters() {
        for (var filterKey : List.of("", "info.id")) {
            var params = new ArrayList<CosmosSqlParameter>();
            assertThat(PGSubQueryExpression.buildArrayContainsAny("items", filterKey, "@items",
                    List.of(), params, "j0")).isEqualTo("(1=0)");
            assertThat(params).isEmpty();
        }
    }


    @Test
    void buildArrayContainsAny_should_work() {

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (data->'items' @> ANY(ARRAY[... ]::jsonb[]))"
             *
             *
             *
             */
            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "", "@items_009", List.of("id001", "id002", "id005"), params, "data");
            assertThat(ret).isEqualTo(" (data->'items' @> ANY(ARRAY[@items_009__0::jsonb, @items_009__1::jsonb, @items_009__2::jsonb]::jsonb[]))");
            assertThat(params).hasSize(3);
            var param = params.stream().collect(Collectors.toList()).get(2);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009__2", "\"id005\"").toJson());
        }

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001"], params
             * OUTPUT:
             * " (data->'items' @> ANY(ARRAY[@items_009__0::jsonb]::jsonb[]))"
             *
             *
             *
             */
            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "", "@items_009", "id001", params, "data");
            assertThat(ret).isEqualTo(" (data->'items' @> ANY(ARRAY[@items_009__0::jsonb]::jsonb[]))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009__0", "\"id001\"").toJson());
        }


        {
            /**
             * INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (data->'items' @> ANY(ARRAY[jsonb_build_array(jsonb_build_object('id', ...)), ...]::jsonb[]))"
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ids = List.of("id001", "id002", "id005");
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "id", "@items_id_010", ids, params, "data");

            assertThat(ret).isEqualTo(" (data->'items' @> ANY(ARRAY[jsonb_build_array(jsonb_build_object('id', @items_id_010__0)), jsonb_build_array(jsonb_build_object('id', @items_id_010__1)), jsonb_build_array(jsonb_build_object('id', @items_id_010__2))]::jsonb[]))");
            assertThat(params).hasSize(3);

            for(var i=0; i<ids.size(); i++) {
                var param = params.stream().collect(Collectors.toList()).get(i);
                assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_id_010__%d".formatted(i), ids.get(i)).toJson());
            }


        }

        {
            /**
             * INPUT: "items", "name", "@items_name_010", "react", params
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "name", "@items_name_010", "react", params, "data");

            assertThat(ret).isEqualTo(" (data->'items' @> ANY(ARRAY[jsonb_build_array(jsonb_build_object('name', @items_name_010__0))]::jsonb[]))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_name_010__0", "react").toJson());

        }
    }


    @Test
    void buildArrayContainsAll_should_work() {


        {
            /**
             * INPUT: "items", "", "@items_009", ["id001", "id002"], params
             * OUTPUT:
             * "  (data->'items' @> @items_009::jsonb)"
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAll("items", "", "@items_009", List.of("id001", "id002"), params, "data");
            assertThat(ret).isEqualTo(" (data->'items' @> @items_009__0::jsonb AND data->'items' @> @items_009__1::jsonb)");
            assertThat(params).hasSize(2);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009__0", "\"id001\"").toJson());
        }

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001"], params
             * OUTPUT:
             * " data->'items' ?? 'id001'
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAll("items", "", "@items_009", List.of("id001"), params, "data");
            assertThat(ret).isEqualTo(" (data->'items' @> @items_009__0::jsonb)");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009__0", "\"id001\"").toJson());
        }


        {
            /**
             * INPUT: "tags", "name", "@tags_name_010", ["react", "java"], params
             * OUTPUT:
             * "  (data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__0)
             * AND data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__1)
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAll("tags", "name", "@tags_name_010", List.of("react", "java"), params, "data");

            assertThat(ret).isEqualTo(" (data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__0)) AND data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__1)))");
            assertThat(params).hasSize(2);
            var ps = List.copyOf(params);
            assertThat(ps.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@tags_name_010__0", "react").toJson());
            assertThat(ps.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@tags_name_010__1", "java").toJson());

        }

        {
            /**
             * INPUT: "items", "name", "@items_name_010", "react", params
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAll("items", "name", "@items_name_010", "react", params, "data");

            assertThat(ret).isEqualTo(" (data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_name_010__0)))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_name_010__0", "react").toJson());

        }
    }


    @Test
    void buildNestedJsonbExpression_should_work() {
        {
            /**
             *  Builds a PostgreSQL JSONB expression for simple keys.
             *  Example: For input "age", "@param000_age", returns:
             *  "jsonb_build_array(jsonb_build_object('age', @param000_age))"
             *
             */

            var ret = PGSubQueryExpression.buildNestedJsonbExpression("school.grade", "@param_grade_001");
            assertThat(ret).isEqualTo("jsonb_build_array(jsonb_build_object('school', jsonb_build_object('grade', @param_grade_001)))");

        }

        {
            /**
             *  Builds a PostgreSQL JSONB expression for nested keys.
             *  Example: For input "school.grade", "@param000_grade", returns:
             *  "jsonb_build_array(jsonb_build_object('school', jsonb_build_object('grade', @param000_grade)))"
             *
             */

            var ret = PGSubQueryExpression.buildNestedJsonbExpression("school.grade", "@param_grade_001");
            assertThat(ret).isEqualTo("jsonb_build_array(jsonb_build_object('school', jsonb_build_object('grade', @param_grade_001)))");

        }

    }


}
