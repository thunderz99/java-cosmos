package io.github.thunderz99.cosmos.condition;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class SubQueryExpressionTest {

    @Test
    void buildArrayContainsAny_should_work() {


        {
            /**
             * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE ARRAY_CONTAINS(@items_009, x)))"
             *
             *
             *
             */
            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAny("items", "", "@items_009", List.of("id001", "id002", "id005"), params);
            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"items\"] WHERE ARRAY_CONTAINS(@items_009, x)))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new SqlParameter("@items_009", List.of("id001", "id002", "id005")).toJson());
        }


        {
            /**
             * INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
             * OUTPUT:
             * " (EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE ARRAY_CONTAINS(@items_id_010, x["id"])))"
             */

            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAny("items", "id", "@items_id_010", List.of("id001", "id002", "id005"), params);

            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"items\"] WHERE ARRAY_CONTAINS(@items_id_010, x[\"id\"])))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new SqlParameter("@items_id_010", List.of("id001", "id002", "id005")).toJson());


        }

        {
            /**
             * INPUT: "items", "name", "@items_name_010", "react", params
             */

            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAny("items", "name", "@items_name_010", "react", params);

            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"items\"] WHERE x[\"name\"] = @items_name_010))");
            assertThat(params).hasSize(1);
            var param = params.stream().collect(Collectors.toList()).get(0);
            assertThat(param.toJson()).isEqualTo(new SqlParameter("@items_name_010", "react").toJson());


        }
    }

    @Test
    void buildArrayContainsAll_should_work() {


        {
            /**
             * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
             *
             */
            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAll("items", "", "@items_009", List.of("id001", "id002"), params);
            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"items\"] WHERE x = @items_009__0) AND EXISTS(SELECT VALUE x FROM x IN c[\"items\"] WHERE x = @items_009__1))");
            assertThat(params).hasSize(2);
            var ps = List.copyOf(params);
            assertThat(ps.get(0).toJson()).isEqualTo(new SqlParameter("@items_009__0","id001").toJson());
            assertThat(ps.get(1).toJson()).isEqualTo(new SqlParameter("@items_009__1","id002").toJson());
        }


        {
            /**
             * INPUT: "tags", "name", "@param001_tags__name", ["react", "java"], params
             */

            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAll("tags", "name", "@param001_tags__name", List.of("react", "java"), params);

            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x[\"name\"] = @param001_tags__name__0) AND EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x[\"name\"] = @param001_tags__name__1))");
            assertThat(params).hasSize(2);
            var ps = List.copyOf(params);
            assertThat(ps.get(0).toJson()).isEqualTo(new SqlParameter("@param001_tags__name__0","react").toJson());
            assertThat(ps.get(1).toJson()).isEqualTo(new SqlParameter("@param001_tags__name__1","java").toJson());

        }

        {
            /**
             * INPUT: "tags", "name", "@param001_tags__name", "react", params
             */

            var params = new SqlParameterCollection();
            var ret = SubQueryExpression.buildArrayContainsAny("tags", "name", "@param001_tags__name", "react", params);

            assertThat(ret).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x[\"name\"] = @param001_tags__name))");
            assertThat(params).hasSize(1);
            var ps = List.copyOf(params);
            assertThat(ps.get(0).toJson()).isEqualTo(new SqlParameter("@param001_tags__name","react").toJson());


        }
    }

    @Test
    void toQuerySpec_should_work() throws Exception {

        {
            //ARRAY_CONTAINS_ANY
            var exp = new SubQueryExpression("tags", "name", List.of("react", "java"), "ARRAY_CONTAINS_ANY");
            var paramIndex = new AtomicInteger(1);
            var querySpec = exp.toQuerySpec(paramIndex);

            assertThat(querySpec.getQueryText()).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE ARRAY_CONTAINS(@param001_tags__name, x[\"name\"])))");
            var params = querySpec.getParameters();
            assertThat(params).hasSize(1);
            var param = List.copyOf(params).get(0);
            assertThat(param.toJson()).isEqualTo(new SqlParameter("@param001_tags__name", List.of("react", "java")).toJson());
        }

        {
            //ARRAY_CONTAINS_ALL
            var exp = new SubQueryExpression("tags", "name", List.of("react", "java"), "ARRAY_CONTAINS_ALL");
            var paramIndex = new AtomicInteger(1);
            var querySpec = exp.toQuerySpec(paramIndex);

            assertThat(querySpec.getQueryText()).isEqualTo(" (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x[\"name\"] = @param001_tags__name__0) AND EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x[\"name\"] = @param001_tags__name__1))");
            var params = querySpec.getParameters();
            assertThat(params).hasSize(2);
            var ps = List.copyOf(params);
            assertThat(ps.get(0).toJson()).isEqualTo(new SqlParameter("@param001_tags__name__0", "react").toJson());
            assertThat(ps.get(1).toJson()).isEqualTo(new SqlParameter("@param001_tags__name__1", "java").toJson());
        }

    }

}