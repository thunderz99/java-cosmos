package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConditionTest {

	@Test
	public void buildQuerySpec_should_get_correct_SQL() {

        var q = Condition.filter("fullName.last", "Hanks", //

                        "id", List.of("id001", "id002", "id005"), //
                        "age", 30) //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) AND (c[\"age\"] = @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 30).toJson());
    }

	@Test
	public void buildQuerySpec_should_get_correct_SQL_for_Count() {

		var q = Condition.filter("fullName.last", "Hanks", //
				"id IN", List.of("id001", "id002", "id005"), //
				"age", 30) //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpecForCount();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT COUNT(1) FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) AND (c[\"age\"] = @param002_age)");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 30).toJson());
    }

	@Test
	public void buildQuerySpec_should_work_for_compare_operator() {

        var q = Condition.filter("fullName.last", "Hanks", //

                        "id IN", List.of("id001", "id002", "id005"), //
                        "age >=", 30, //
                        "fullName.last !=", "ABC") //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) AND (c[\"age\"] >= @param002_age) AND (c[\"fullName\"][\"last\"] != @param003_fullName__last) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 30).toJson());
        assertThat(params.get(3).toJson()).isEqualTo(new CosmosSqlParameter("@param003_fullName__last", "ABC").toJson());
    }

	@Test
	public void buildQuerySpec_should_work_for_str_operator() {

        var q = Condition.filter("fullName.last", "Hanks", //

                        "id IN", List.of("id001", "id002", "id005"), //
                        "age", 30, //
                        "fullName.first OR fullName.last STARTSWITH", "F", //
                        "fullName.last CONTAINS", "L", //
                        "skill ARRAY_CONTAINS", "Java") //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) AND (c[\"age\"] = @param002_age) AND ( (STARTSWITH(c[\"fullName\"][\"first\"], @param003_fullName__first)) OR (STARTSWITH(c[\"fullName\"][\"last\"], @param004_fullName__last)) ) AND (CONTAINS(c[\"fullName\"][\"last\"], @param005_fullName__last)) AND (ARRAY_CONTAINS(c[\"skill\"], @param006_skill)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 30).toJson());
        assertThat(params.get(3).toJson()).isEqualTo(new CosmosSqlParameter("@param003_fullName__first", "F").toJson());
        assertThat(params.get(4).toJson()).isEqualTo(new CosmosSqlParameter("@param004_fullName__last", "F").toJson());
        assertThat(params.get(5).toJson()).isEqualTo(new CosmosSqlParameter("@param005_fullName__last", "L").toJson());
        assertThat(params.get(6).toJson()).isEqualTo(new CosmosSqlParameter("@param006_skill", "Java").toJson());
    }

	@Test
	public void buildQuerySpec_should_generate_SQL_for_fields() {

        var q = Condition.filter("fullName.last", "Hanks", //

                        "id IN", List.of("id001", "id002", "id005"), //
                        "age", 30) //
                .fields("id", "fullName.first", "age") //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo("SELECT VALUE {\"id\":c[\"id\"],\"fullName\":{\"first\":c[\"fullName\"][\"first\"]},\"age\":c[\"age\"]} FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) AND (c[\"age\"] = @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20"
        );

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 30).toJson());
    }

	@Test
	public void buildQuerySpec_should_generate_SQL_for_nested_fields() {

		var q = Condition.filter("fullName.last", "Hanks") //
				.fields("contents.aa-bb-cc", "contents.xx-yy-zz", "age") //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT VALUE {\"contents\":{\"aa-bb-cc\":c[\"contents\"][\"aa-bb-cc\"],\"xx-yy-zz\":c[\"contents\"][\"xx-yy-zz\"]},\"age\":c[\"age\"]} FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params).hasSize(1);
        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
    }

	@Test
	public void generate_field_should_work() {
		assertThat(Condition.generateSelectByFields(Set.of("organization.primary-leader.name")))
				.isEqualTo("VALUE {\"organization\":{\"primary-leader\":{\"name\":c[\"organization\"][\"primary-leader\"][\"name\"]}}}");
	}

	@Test
	public void generate_field_should_throw_when_invalid_field() {
		for (var ch : List.of("{", "}", ",", "\"", "'")) {
			assertThatThrownBy(() -> Condition.generateSelectByFields(Set.of(ch))).isInstanceOf(IllegalArgumentException.class)
					.hasMessageContaining("field cannot").hasMessageContaining(ch);

		}

	}

	@Test
	public void buildQuerySpec_should_work_for_sub_cond_or() {
        {
            var q = Condition.filter("fullName.last", "Hanks", //
                    SubConditionType.OR,
                    List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")), //
                    "age", 30) //
                    .sort("_ts", "DESC") //
                    .offset(10) //
                    .limit(20) //
                    .toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND ((c[\"position\"] = @param001_position) OR (c[\"organization\"] = @param002_organization)) AND (c[\"age\"] = @param003_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

            var params = List.copyOf(q.getParameters());

            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_position", "leader").toJson());
            assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_organization", "executive").toJson());
            assertThat(params.get(3).toJson()).isEqualTo(new CosmosSqlParameter("@param003_age", 30).toJson());
        }

        {
            //sub query in single condition without a List
            var q = Condition.filter(SubConditionType.OR,
                    Condition.filter("position", "leader"))
                    .toQuerySpec();
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }

        {
            //multiple SUB_COND_OR s
            var q = Condition.filter(
                            SubConditionType.OR,
                            Condition.filter("position", "leader"),
                            SubConditionType.OR + " 2",
                            Condition.filter("address", "London")
                    )
                    .toQuerySpec();
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) AND ((c[\"address\"] = @param001_address)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_address", "London").toJson());
        }
    }

    @Test
    public void buildQuerySpec_should_work_for_and_nested_with_or() {

        var q = Condition.filter("isChecked", true,
                        "$OR or_first ",
                        List.of(Condition.filter("id_A", "value_A"),
                                Condition.filter("id_B", "value_B", "id_C", "value_C")))
                .toQuerySpec();


        assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE (c[\"isChecked\"] = @param000_isChecked) AND ((c[\"id_A\"] = @param001_id_A) OR (c[\"id_B\"] = @param002_id_B) AND (c[\"id_C\"] = @param003_id_C)) OFFSET 0 LIMIT 100");
    }

    @Test
    public void buildQuerySpec_should_work_for_sub_cond_or_from_the_beginning() {

        var q = Condition.filter( //
                        SubConditionType.OR,
                        List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
                ) //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE ((c[\"position\"] = @param000_position) OR (c[\"organization\"] = @param001_organization)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_organization", "executive").toJson());
    }


	@Test
	public void buildQuerySpec_should_work_for_sub_cond_and() {

        {
            // SUB_COND_AND mixed with sort, offset, limit
            var q = Condition.filter( //
                    SubConditionType.AND,
                    List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
            ) //
                    .sort("_ts", "DESC") //
                    .offset(10) //
                    .limit(20) //
                    .toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM c WHERE ((c[\"position\"] = @param000_position) AND (c[\"organization\"] = @param001_organization)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

            var params = List.copyOf(q.getParameters());

            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_organization", "executive").toJson());
        }

        {
            //sub query in single condition without a List
            var q = Condition.filter(SubConditionType.AND,
                    Condition.filter("position", "leader"))
                    .toQuerySpec();
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }

        {
            //multiple SUB_COND_AND s
            var q = Condition.filter(
                    SubConditionType.AND,
                    Condition.filter("position", "leader"),
                    SubConditionType.AND + " another",
                    List.of(Condition.rawSql("1=1"))
            )
                    .toQuerySpec();
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) AND (1=1) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }
    }

	@Test
	public void buildQuerySpec_should_work_for_empty() {

		var q = Condition.filter().toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c OFFSET 0 LIMIT 100");

		assertThat(q.getParameters()).isEmpty();

	}

	@Test
	public void buildQuerySpec_should_work_for_empty_sub_query() {

        {
            var q = Condition.filter(SubConditionType.OR, //
                    List.of()).toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).isEmpty();
        }

        {
            var q = Condition.filter(SubConditionType.OR, //
                    List.of(Condition.filter("id", 1))).toQuerySpec();

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM c WHERE ((c[\"id\"] = @param000_id)) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var q = Condition.filter("id", 1, SubConditionType.AND, //
                    List.of()).toQuerySpec();

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM c WHERE (c[\"id\"] = @param000_id) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var q = Condition.filter(SubConditionType.AND, //
                    List.of(Condition.filter(), Condition.filter())).toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(0);
        }

        {
            var q = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter())).toQuerySpec();

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM c WHERE (c[\"id\"] = @param000_id) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var q = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter(), Condition.filter("name", "Tom"))).toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM c WHERE (c[\"id\"] = @param000_id) AND ((c[\"name\"] = @param001_name)) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(2);
        }

    }

	@Test
	public void buildQuerySpec_should_work_for_raw_cond() {

        {
            // use SUB_COND_AND and Condition.rawSql
            var q = Condition.filter("open", true, //
                    SubConditionType.AND, //
                    List.of(Condition.rawSql("1=1"))).toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (c[\"open\"] = @param000_open) AND (1=1) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);

        }

        {
            // use SUB_COND_AND and Condition.rawSql

            var params = new SqlParameterCollection(new SqlParameter("@raw_param_status", "%enroll%"));

            var q = Condition.filter("open", true, //
                    SubConditionType.AND, //
                    List.of(Condition.rawSql("c.status LIKE @raw_param_status", params))).toQuerySpec();

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (c[\"open\"] = @param000_open) AND (c.status LIKE @raw_param_status) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(2);

            var valueMap = Map.of("@raw_param_status", "%enroll%", "@param000_open", true);
            q.getParameters().forEach(param -> {
                String paramName = param.getName();
                assertThat(paramName.equals("@param000_open") || paramName.equals("@raw_param_status"));

                assertThat(param.getValue()).isEqualTo(valueMap.get(paramName));
            });

        }

	}


	@Test
	public void buildQuerySpec_should_work_empty_list() {

		{
			var q = Condition.filter("id", List.of()).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (1=0) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).isEmpty();
		}

		{
			var q = Condition.filter("id", List.of(), "name", "Tom").toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (1=0) AND (c[\"name\"] = @param001_name) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
		}

		{
			var q = Condition.filter("name", "Tom", "id", List.of()).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (c[\"name\"] = @param000_name) AND (1=0) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(1);
		}


	}

	@Test
	public void buildQuerySpec_should_work_for_LIKE() {

        var q = Condition.filter("fullName.last", "Hanks", //
                        "fullName.first LIKE", "%om%", //
                        "age >", 20 //
                )
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (c[\"fullName\"][\"first\"] LIKE @param001_fullName__first) AND (c[\"age\"] > @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_fullName__first", "%om%").toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 20).toJson());
    }

	@Test
	public void buildQuerySpec_should_work_for_ARRAY_CONTAINS_ANY() {

        var q = Condition.filter("fullName.last", "Hanks", //
                        "tags ARRAY_CONTAINS_ANY", List.of("id001", "id002", "id005"), //
                        "age >", 20 //
                )
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE ARRAY_CONTAINS(@param001_tags, x))) AND (c[\"age\"] > @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_tags", List.of("id001", "id002", "id005")).toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 20).toJson());
    }

	@Test
	public void buildQuerySpec_should_work_for_ARRAY_CONTAINS_ALL() {

        var q = Condition.filter("fullName.last", "Hanks", //
                        "tags ARRAY_CONTAINS_ALL", List.of("id001", "id002"), //
                        "age >", 20 //
                )
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x = @param001_tags__0) AND EXISTS(SELECT VALUE x FROM x IN c[\"tags\"] WHERE x = @param001_tags__1)) AND (c[\"age\"] > @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_tags__0", "id001").toJson());
        assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param001_tags__1", "id002").toJson());
        assertThat(params.get(3).toJson()).isEqualTo(new CosmosSqlParameter("@param002_age", 20).toJson());
    }

	@Test
	public void generateAggregateSelect_should_work() {
		{
			var aggregate = Aggregate.function("COUNT(1)");
			assertThat(Condition.generateAggregateSelect(aggregate)).isEqualTo("COUNT(1)");
		}
		{
			var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("status", "location.state");
			assertThat(Condition.generateAggregateSelect(aggregate)).isEqualTo("COUNT(1) AS facetCount, c[\"status\"], c[\"location\"][\"state\"]");
		}
	}

	@Test
	public void buildQuerySpec_should_work_for_aggregate() {

		{
			//pure aggregate
			var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("status", "location");
			var q = Condition.filter() //
					.toQuerySpec(aggregate);

			assertThat(q.getQueryText().trim()).isEqualTo(
					"SELECT COUNT(1) AS facetCount, c[\"status\"], c[\"location\"] FROM c GROUP BY c[\"status\"], c[\"location\"] OFFSET 0 LIMIT 100");

			var params = List.copyOf(q.getParameters());
			assertThat(params).isEmpty();

		}

        {
            // with filter
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("status", "location");
            var q = Condition.filter("age >=", 20) //
                    .toQuerySpec(aggregate);

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT COUNT(1) AS facetCount, c[\"status\"], c[\"location\"] FROM c WHERE (c[\"age\"] >= @param000_age) GROUP BY c[\"status\"], c[\"location\"] OFFSET 0 LIMIT 100");

            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_age", 20).toJson());

        }

        {
            // with offset / limit
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("status", "location");
            var q = Condition.filter("age >=", 20).offset(5).limit(10) //
                    .toQuerySpec(aggregate);

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT COUNT(1) AS facetCount, c[\"status\"], c[\"location\"] FROM c WHERE (c[\"age\"] >= @param000_age) GROUP BY c[\"status\"], c[\"location\"] OFFSET 5 LIMIT 10");

            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_age", 20).toJson());

        }

        {
            // with order by
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("status", "location");
            var q = Condition.filter("age >=", 20).sort("status", "ASC") //
                    .toQuerySpec(aggregate);

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM (SELECT COUNT(1) AS facetCount, c[\"status\"], c[\"location\"] FROM c WHERE (c[\"age\"] >= @param000_age) GROUP BY c[\"status\"], c[\"location\"]) agg ORDER BY agg[\"status\"] ASC OFFSET 0 LIMIT 100");

            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_age", 20).toJson());

        }
	}

	@Test
	public void isTrueFalseCondition_should_work() {
		{
			//true condition
			assertThat(Condition.isTrueCondition(Condition.filter())).isFalse();
			assertThat(Condition.isTrueCondition(Condition.filter("q", "1"))).isFalse();
			assertThat(Condition.isTrueCondition(Condition.rawSql("c.id = 001"))).isFalse();
			assertThat(Condition.isTrueCondition(Condition.rawSql(Condition.COND_SQL_FALSE))).isFalse();
			assertThat(Condition.isTrueCondition(Condition.rawSql(Condition.COND_SQL_TRUE))).isTrue();

		}
		{
			//false condition
			assertThat(Condition.isFalseCondition(Condition.filter())).isFalse();
			assertThat(Condition.isFalseCondition(Condition.filter("q", "1"))).isFalse();
			assertThat(Condition.isFalseCondition(Condition.rawSql("c.id = 001"))).isFalse();
			assertThat(Condition.isFalseCondition(Condition.rawSql(Condition.COND_SQL_TRUE))).isFalse();
			assertThat(Condition.isFalseCondition(Condition.rawSql(Condition.COND_SQL_FALSE))).isTrue();

		}
	}

	@Test
	public void extractSubQueries_should_work() {

        assertThat(Condition.extractSubQueries(null)).hasSize(0);

        assertThat(Condition.extractSubQueries(Lists.newArrayList(null, null))).hasSize(0);

        {
            var subQueries = Condition.extractSubQueries(Condition.filter("a", "b"));
            assertThat(subQueries).hasSize(1);
            assertThat(subQueries.get(0).filter).containsEntry("a", "b");
        }
        {
            // using conditions
            var subQueries = Condition.extractSubQueries(List.of(Condition.filter("c", "d")));
            assertThat(subQueries).hasSize(1);
            assertThat(subQueries.get(0).filter).containsEntry("c", "d");
        }
        {
            // using maps
            var subQueries = Condition.extractSubQueries(List.of(Map.of("c", "d")));
            assertThat(subQueries).hasSize(1);
            assertThat(subQueries.get(0).filter).containsEntry("c", "d");
        }
        {
            // using conditions with a raw filter
            var subQueries = Condition.extractSubQueries(List.of(Condition.filter("e", "f"), Condition.trueCondition()));
            assertThat(subQueries).hasSize(2);
            assertThat(subQueries.get(0).filter).containsEntry("e", "f");
            assertThat(Condition.isTrueCondition(subQueries.get(1))).isTrue();
        }
        {
            // using conditions combined with a map and a raw filter
            var subQueries = Condition.extractSubQueries(List.of(Map.of("e", "f"), Condition.trueCondition()));
            assertThat(subQueries).hasSize(2);
            assertThat(subQueries.get(0).filter).containsEntry("e", "f");
            assertThat(Condition.isTrueCondition(subQueries.get(1))).isTrue();
        }

    }

	@Test
	public void copy_should_support_nested_conditions() {

        var cond = Condition.filter("id", "ID001", SubConditionType.OR, List.of(
                Condition.filter("name", "Tom"),
                Condition.filter("age >", "20")
        ));

        var copy = cond.copy();
        assertThat(copy.filter.get("id")).isEqualTo("ID001");
        var subCondObjs = copy.filter.get(SubConditionType.OR);
        assertThat(subCondObjs instanceof List<?>).isTrue();

        if (subCondObjs instanceof List<?>) {
            var list = (List<?>) subCondObjs;
            for (var subCondObj : list) {
                assertThat(subCondObj instanceof Condition).isTrue();
            }
        }
    }

	@Test
	public void copy_should_work() {

		{
			// normal case
			var skillSet = Set.of("java", "python");
			var original = Condition.filter("id", "ID001", "aaa-bbb.value IS_DEFINED", true, "int", 10, "skills ARRAY_CONTAINS_ANY", skillSet)
					.offset(40).limit(20).sort("id", "DESC").crossPartition(true).fields("id", "_ts");
			var copy = original.copy();

			assertThat(copy.filter.get("id")).isEqualTo("ID001");
			assertThat(copy.filter.get("int")).isEqualTo(10);
			assertThat(copy.filter.get("skills ARRAY_CONTAINS_ANY")).isEqualTo(new ArrayList<>(skillSet));
			assertThat(copy.filter.get("aaa-bbb.value IS_DEFINED")).isEqualTo(true);

			assertThat(copy.offset).isEqualTo(original.offset);
			assertThat(copy.limit).isEqualTo(original.limit);
			assertThat(copy.fields).isEqualTo(original.fields);
			assertThat(copy.sort).isEqualTo(original.sort);
			assertThat(copy.crossPartition).isEqualTo(original.crossPartition);

			// deep copy, so change the copy wont affect the original
			copy.filter.put("copy", true);
			assertThat(original.filter).doesNotContainEntry("copy", true);

			copy.fields.clear();
			assertThat(original.fields).hasSize(2);
			assertThat(original.fields).contains("id", "_ts");

			copy.sort.set(0, "updatedAt");
			assertThat(original.sort.get(0)).isEqualTo("id");


		}
		{
			// for empty filter
			var original = Condition.filter();
			var copy = original.copy();

			// deep copy test
			copy.filter.put("id", "ID002");
			assertThat(original.filter).hasSize(0).doesNotContainEntry("id", "ID002");

			copy.fields.add("id");
			copy.fields.add("createdAt");
			assertThat(original.fields).isEmpty();

			copy.sort.add("_ts");
			copy.sort.add("DESC");

			assertThat(original.sort).isEmpty();

		}

	}

	@Test
	public void copy_should_work_for_rawsql() {
		{
			//simple cond
			var original = Condition.trueCondition();
			var copy = original.copy();
			assertThat(copy).isNotNull();
			assertThat(copy.rawQuerySpec.getQueryText()).isEqualTo("1=1");
		}
		{
			//complex cond with sql parameters

			var param1 = new SqlParameter("@param01", "Tom");
			var param2 = new SqlParameter("@param02", 20);


			var original = Condition.rawSql("SELECT * from c WHERE c.name = @param01 AND c.age > @param02", new SqlParameterCollection(param1, param2));

			var copy = original.copy();
			assertThat(copy).isNotNull();
			assertThat(copy.rawQuerySpec.getQueryText()).isEqualTo("SELECT * from c WHERE c.name = @param01 AND c.age > @param02");

			var params = new ArrayList<>(copy.rawQuerySpec.getParameters());
			assertThat(params).hasSize(2);
			for (var param : params) {
				assertThat(param.getName()).startsWith("@param0");
				if (param.getName().endsWith("01")) {
                    assertThat(param.getValue()).isEqualTo("Tom");
				} else {
                    assertThat(param.getValue()).isEqualTo(20);
				}
			}
		}
	}

    public enum Status {
        enrolled, suspended, retired
    }

    @Test
    public void copy_should_work_for_enum() {

        var cond = Condition.filter("id", "ID001", "status", Status.enrolled);

        var copy = cond.copy();
        assertThat(copy.filter.get("id")).isEqualTo("ID001");
        assertThat(copy.filter.get("status")).isEqualTo(Status.enrolled);

    }

	@Test
	public void buildQuerySpec_should_work_for_is_defined() {

        var q = Condition.filter("id", "Hanks", //
                        "age IS_DEFINED", true) //
                .limit(20)
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"id\"] = @param000_id) AND (IS_DEFINED(c[\"age\"]) = @param001_age) OFFSET 0 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_id", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_age", true).toJson());
    }

	@Test
	public void negative_should_have_no_effect_for_a_whole_rawSql() {

		var q = Condition.rawSql("SELECT * FROM c WHERE \"tokyo\" IN c.cities OFFSET 0 LIMIT 20") //
				.not() // negative will have no effect for a whole rawSql
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE \"tokyo\" IN c.cities OFFSET 0 LIMIT 20");

	}

	@Test
    public void negative_should_have_effect_for_a_condition_rawSql() {

        var filterQuery = Condition.rawSql("\"tokyo\" IN c.cities") //
                .not() // negative will have no effect for a whole rawSql
                .generateFilterQuery("", Lists.newArrayList(), new AtomicInteger(), new AtomicInteger());

        assertThat(filterQuery.queryText.toString()).isEqualTo(
                " NOT(\"tokyo\" IN c.cities)");

    }

    @Test
    public void buildQuerySpec_should_work_for_IN_operator_followed_by_str() {

        var q = Condition.filter("fullName.last", "Hanks", //
                "id IN", "id001") //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20) //
                .toQuerySpec();

        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (ARRAY_CONTAINS(@param001_id, c[\"id\"])) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_id", List.of("id001")).toJson());
    }

    @Test
    public void processNegativeQuery_should_work() {
        assertThat(Condition.processNegativeQuery(null, true)).isNull();
        assertThat(Condition.processNegativeQuery(null, false)).isNull();
        assertThat(Condition.processNegativeQuery("", true)).isEmpty();
        assertThat(Condition.processNegativeQuery("", false)).isEmpty();

        assertThat(Condition.processNegativeQuery("c.age >= 1", false)).isEqualTo("c.age >= 1");
        assertThat(Condition.processNegativeQuery("c.age >= 1", true)).isEqualTo(" NOT(c.age >= 1)");

	}

	@Test
	public void true_condition_in_subquery_should_work() {

        var cond1 = Condition.trueCondition();
        var cond2 = Condition.filter("id", "001");

        var cond = Condition.filter(SubConditionType.AND, List.of(cond1, cond2));

        var q = cond.toQuerySpec();

        assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE (1=1 AND (c[\"id\"] = @param000_id)) OFFSET 0 LIMIT 100");

        var params = List.copyOf(q.getParameters());

        assertThat(params).hasSize(1);

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_id", "001").toJson());

    }

    @Test
    void typeCheckFunctionPattern_should_work() {
        assertThat(Condition.typeCheckFunctionPattern.asMatchPredicate().test("IS_NUMBER")).isTrue();
        assertThat(Condition.typeCheckFunctionPattern.asMatchPredicate().test("IS_DEFINED")).isTrue();
        assertThat(Condition.typeCheckFunctionPattern.asMatchPredicate().test("IS_PRIMITIVE")).isTrue();
        assertThat(Condition.typeCheckFunctionPattern.asMatchPredicate().test("IS_NOTEXIST")).isFalse();
    }

    @Test
    void getRawQuerySpecJson_should_work() {
        assertThat(Condition.filter().getRawQuerySpecJson()).isEqualTo(null);

        assertThat(Condition.falseCondition().getRawQuerySpecJson())
                .isEqualTo(JsonUtil.toJson(Condition.rawSql("1=0").rawQuerySpec));
        assertThat(Condition.trueCondition().getRawQuerySpecJson())
                .isEqualTo(JsonUtil.toJson(Condition.rawSql("1=1").rawQuerySpec));
    }

}
