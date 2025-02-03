package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.SubQueryExpression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PGSubQueryExpressionTest {


    @Test
    void buildArrayContainsAny_should_work() {

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (data->'items' ??| @items_009)"
             *
             *
             *
             */
            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "", "@items_009", List.of("id001", "id002", "id005"), params, "data");
            assertThat(ret).isEqualTo(" (data->'items' ??| @items_009)");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009", List.of("id001", "id002", "id005")).toJson());
        }

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001"], params
             * OUTPUT:
             * " (data->'items' ?? @items_009)"
             *
             *
             *
             */
            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "", "@items_009", "id001", params, "data");
            assertThat(ret).isEqualTo(" (data->'items' ?? @items_009)");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009", "id001").toJson());
        }


        {
            /**
             * INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (data->'items' @> '[{"id": @items_id_010__0}]' OR data->'items' @> '[{"id": @items_id_010__1}]' OR data->'items' @> '[{"id": @items_id_010__2}]')"
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ids = List.of("id001", "id002", "id005");
            var ret = PGSubQueryExpression.buildArrayContainsAny("items", "id", "@items_id_010", ids, params, "data");

            assertThat(ret).isEqualTo(" (data->'items' @> jsonb_build_array(jsonb_build_object('id', @items_id_010__0)) OR data->'items' @> jsonb_build_array(jsonb_build_object('id', @items_id_010__1)) OR data->'items' @> jsonb_build_array(jsonb_build_object('id', @items_id_010__2)))");
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

            assertThat(ret).isEqualTo(" (data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_name_010__0)))");
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
            assertThat(ret).isEqualTo(" (data->'items' @> @items_009::jsonb)");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009", JsonUtil.toJson(List.of("id001", "id002"))).toJson());
        }

        {
            /**
             * INPUT: "items", "", "@items_009", ["id001"], params
             * OUTPUT:
             * " data->'items' ?? 'id001'
             */

            var params = new ArrayList<CosmosSqlParameter>();
            var ret = PGSubQueryExpression.buildArrayContainsAll("items", "", "@items_009", List.of("id001"), params, "data");
            assertThat(ret).isEqualTo(" (data->'items' ?? @items_009)");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new CosmosSqlParameter("@items_009", "id001").toJson());
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