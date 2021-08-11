package io.github.thunderz99.cosmos.condition;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.thunderz99.cosmos.condition.Condition.SubConditionType;
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

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id", List.of("id001", "id002", "id005")).toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
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
				"SELECT COUNT(1) FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (c[\"id\"] IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c[\"age\"] = @param002_age)");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_id__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param001_id__2", "id005").toJson());
		assertThat(params.get(4).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
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
				"SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (c[\"id\"] IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c[\"age\"] >= @param002_age) AND (c[\"fullName\"][\"last\"] != @param003_fullName__last) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_id__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param001_id__2", "id005").toJson());
		assertThat(params.get(4).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
		assertThat(params.get(5).toJson()).isEqualTo(new SqlParameter("@param003_fullName__last", "ABC").toJson());
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
				"SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (c[\"id\"] IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c[\"age\"] = @param002_age) AND ( (STARTSWITH(c[\"fullName\"][\"first\"], @param003_fullName__first)) OR (STARTSWITH(c[\"fullName\"][\"last\"], @param004_fullName__last)) ) AND (CONTAINS(c[\"fullName\"][\"last\"], @param005_fullName__last)) AND (ARRAY_CONTAINS(c[\"skill\"], @param006_skill)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_id__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param001_id__2", "id005").toJson());
		assertThat(params.get(4).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
		assertThat(params.get(5).toJson()).isEqualTo(new SqlParameter("@param003_fullName__first", "F").toJson());
		assertThat(params.get(6).toJson()).isEqualTo(new SqlParameter("@param004_fullName__last", "F").toJson());
		assertThat(params.get(7).toJson()).isEqualTo(new SqlParameter("@param005_fullName__last", "L").toJson());
		assertThat(params.get(8).toJson()).isEqualTo(new SqlParameter("@param006_skill", "Java").toJson());
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

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT VALUE {\"id\":c.id, \"fullName\":{\"first\":c.fullName.first}, \"age\":c.age} FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND (c[\"id\"] IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c[\"age\"] = @param002_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_id__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param001_id__2", "id005").toJson());
		assertThat(params.get(4).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
	}

	@Test
	public void generate_field_should_work() {
		assertThat(Condition.generateOneFieldSelect("org.leader.name"))
				.isEqualTo("\"org\":{\"leader\":{\"name\":c.org.leader.name}}");
	}

	@Test
	public void generate_field_should_throw_when_invalid_field() {
		for (var ch : List.of("{", "}", ",", "\"", "'")) {
			assertThatThrownBy(() -> Condition.generateOneFieldSelect(ch)).isInstanceOf(IllegalArgumentException.class)
					.hasMessageContaining("field cannot").hasMessageContaining(ch);

		}

	}

	@Test
	public void buildQuerySpec_should_work_for_sub_cond_or() {
		{
			var q = Condition.filter("fullName.last", "Hanks", //
					SubConditionType.SUB_COND_OR,
					List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")), //
					"age", 30) //
					.sort("_ts", "DESC") //
					.offset(10) //
					.limit(20) //
					.toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo(
					"SELECT * FROM c WHERE (c[\"fullName\"][\"last\"] = @param000_fullName__last) AND ((c[\"position\"] = @param001_position) OR (c[\"organization\"] = @param002_organization)) AND (c[\"age\"] = @param003_age) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

			var params = List.copyOf(q.getParameters());

			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
			assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_position", "leader").toJson());
			assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param002_organization", "executive").toJson());
			assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param003_age", 30).toJson());
		}

		{
			//sub query in single condition without a List
			var q = Condition.filter(SubConditionType.SUB_COND_OR,
					Condition.filter("position", "leader"))
					.toQuerySpec();
			assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) OFFSET 0 LIMIT 100");
			var params = List.copyOf(q.getParameters());
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
		}

		{
			//multiple SUB_COND_OR s
			var q = Condition.filter(
					SubConditionType.SUB_COND_OR,
					Condition.filter("position", "leader"),
					SubConditionType.SUB_COND_OR + " 2",
					Condition.filter("address", "London")
			)
					.toQuerySpec();
			assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) AND ((c[\"address\"] = @param001_address)) OFFSET 0 LIMIT 100");
			var params = List.copyOf(q.getParameters());
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
			assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_address", "London").toJson());
		}
	}

	@Test
	public void buildQuerySpec_should_work_for_sub_cond_or_from_the_beginning() {

		var q = Condition.filter( //
				SubConditionType.SUB_COND_OR,
				List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
		) //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE ((c[\"position\"] = @param000_position) OR (c[\"organization\"] = @param001_organization)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_organization", "executive").toJson());
	}


	@Test
	public void buildQuerySpec_should_work_for_sub_cond_and() {

		{
			// SUB_COND_AND mixed with sort, offset, limit
			var q = Condition.filter( //
					SubConditionType.SUB_COND_AND,
					List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
			) //
					.sort("_ts", "DESC") //
					.offset(10) //
					.limit(20) //
					.toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo(
					"SELECT * FROM c WHERE ((c[\"position\"] = @param000_position) AND (c[\"organization\"] = @param001_organization)) ORDER BY c[\"_ts\"] DESC OFFSET 10 LIMIT 20");

			var params = List.copyOf(q.getParameters());

			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
			assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_organization", "executive").toJson());
		}

		{
			//sub query in single condition without a List
			var q = Condition.filter(SubConditionType.SUB_COND_AND,
					Condition.filter("position", "leader"))
					.toQuerySpec();
			assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) OFFSET 0 LIMIT 100");
			var params = List.copyOf(q.getParameters());
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
		}

		{
			//multiple SUB_COND_AND s
			var q = Condition.filter(
					SubConditionType.SUB_COND_AND,
					Condition.filter("position", "leader"),
					SubConditionType.SUB_COND_AND + " another",
					List.of(Condition.rawSql("1=1"))
			)
					.toQuerySpec();
			assertThat(q.getQueryText()).isEqualTo("SELECT * FROM c WHERE ((c[\"position\"] = @param000_position)) AND (1=1) OFFSET 0 LIMIT 100");
			var params = List.copyOf(q.getParameters());
			assertThat(params).hasSize(1);
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
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
			var q = Condition.filter(SubConditionType.SUB_COND_OR, //
					List.of()).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).isEmpty();
		}

		{
			var q = Condition.filter(SubConditionType.SUB_COND_OR, //
					List.of(Condition.filter("id", 1))).toQuerySpec();

			assertThat(q.getQueryText().trim())
					.isEqualTo("SELECT * FROM c WHERE ((c[\"id\"] = @param000_id)) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(1);
		}

		{
			var q = Condition.filter("id", 1, SubConditionType.SUB_COND_AND, //
					List.of()).toQuerySpec();

			assertThat(q.getQueryText().trim())
					.isEqualTo("SELECT * FROM c WHERE (c[\"id\"] = @param000_id) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(1);
		}

		{
			var q = Condition.filter(SubConditionType.SUB_COND_AND, //
					List.of(Condition.filter(), Condition.filter())).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(0);
		}

		{
			var q = Condition.filter("id", 1, SubConditionType.SUB_COND_OR, //
					List.of(Condition.filter())).toQuerySpec();

			assertThat(q.getQueryText().trim())
					.isEqualTo("SELECT * FROM c WHERE (c[\"id\"] = @param000_id) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(1);
		}

		{
			var q = Condition.filter("id", 1, SubConditionType.SUB_COND_OR, //
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
					SubConditionType.SUB_COND_AND, //
					List.of(Condition.rawSql("1=1"))).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (c[\"open\"] = @param000_open) AND (1=1) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(1);

		}

		{
			// use SUB_COND_AND and Condition.rawSql

			var params = new SqlParameterCollection(new SqlParameter("@raw_param_status", "%enroll%"));

			var q = Condition.filter("open", true, //
					SubConditionType.SUB_COND_AND, //
					List.of(Condition.rawSql("c.status LIKE @raw_param_status", params))).toQuerySpec();

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (c[\"open\"] = @param000_open) AND (c.status LIKE @raw_param_status) OFFSET 0 LIMIT 100");
			assertThat(q.getParameters()).hasSize(2);

			var valueMap = Map.of("@raw_param_status", "%enroll%", "@param000_open", true);
			q.getParameters().forEach( param -> {
				String paramName = param.getName();
				assertThat(paramName.equals("@param000_open") || paramName.equals("@raw_param_status"));

				assertThat(param.getValue(Object.class)).isEqualTo(valueMap.get(paramName));
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

			assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM c WHERE (1=0) AND (c[\"name\"] = @param000_name) OFFSET 0 LIMIT 100");
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

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_fullName__first", "%om%").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param002_age", 20).toJson());
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

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_tags", List.of("id001", "id002", "id005")).toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param002_age", 20).toJson());
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

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_tags__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_tags__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param002_age", 20).toJson());
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
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_age", 20).toJson());

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
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_age", 20).toJson());

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
			assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_age", 20).toJson());

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
		{
			var subQueries = Condition.extractSubQueries(Condition.filter("a", "b"));
			assertThat(subQueries).hasSize(1);
			assertThat(subQueries.get(0).filter).containsEntry("a", "b");
		}
		{
			var subQueries = Condition.extractSubQueries(List.of(Condition.filter("c", "d")));
			assertThat(subQueries).hasSize(1);
			assertThat(subQueries.get(0).filter).containsEntry("c", "d");
		}
		{
			var subQueries = Condition.extractSubQueries(List.of(Condition.filter("e", "f"), Condition.trueCondition()));
			assertThat(subQueries).hasSize(2);
			assertThat(subQueries.get(0).filter).containsEntry("e", "f");
			assertThat(Condition.isTrueCondition(subQueries.get(1))).isTrue();
		}

	}

	@Test
	public void copy_should_support_nested_conditions() {

		var cond = Condition.filter("id", "ID001", SubConditionType.SUB_COND_OR.name(), List.of(
				Condition.filter("name", "Tom"),
				Condition.filter("age >", "20")
		));

		var copy = cond.copy();
		assertThat(copy.filter.get("id")).isEqualTo("ID001");
		var subCondObjs = copy.filter.get(SubConditionType.SUB_COND_OR.name());
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

		var skillSet = Set.of("java", "python");
		var cond = Condition.filter("id", "ID001", "aaa-bbb.value IS_DEFINED", true, "int", 10, "skills ARRAY_CONTAINS_ANY", skillSet)
				.offset(40).limit(20).sort("id", "DESC").crossPartition(true).fields("id", "_ts");
		var copy = cond.copy();

		assertThat(copy.filter.get("id")).isEqualTo("ID001");
		assertThat(copy.filter.get("int")).isEqualTo(10);
		assertThat(copy.filter.get("skills ARRAY_CONTAINS_ANY")).isEqualTo(new ArrayList<>(skillSet));
		assertThat(copy.filter.get("aaa-bbb.value IS_DEFINED")).isEqualTo(true);

		assertThat(copy.offset).isEqualTo(cond.offset);
		assertThat(copy.limit).isEqualTo(cond.limit);
		assertThat(copy.fields).isEqualTo(cond.fields);
		assertThat(copy.sort).isEqualTo(cond.sort);
		assertThat(copy.crossPartition).isEqualTo(cond.crossPartition);

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

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_id", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_age", true).toJson());
	}
}
