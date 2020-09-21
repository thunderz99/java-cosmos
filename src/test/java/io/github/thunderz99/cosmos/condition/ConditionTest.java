package io.github.thunderz99.cosmos.condition;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.microsoft.azure.documentdb.SqlParameter;

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
				"SELECT * FROM c WHERE (c.fullName.last = @param000_fullName__last) AND (c.id IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c.age = @param002_age) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_id__0", "id001").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param001_id__1", "id002").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param001_id__2", "id005").toJson());
		assertThat(params.get(4).toJson()).isEqualTo(new SqlParameter("@param002_age", 30).toJson());
	}

	@Test
	public void buildQuerySpec_should_get_correct_SQL_for_Count() {

		var q = Condition.filter("fullName.last", "Hanks", //
				"id", List.of("id001", "id002", "id005"), //
				"age", 30) //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpecForCount();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT COUNT(1) FROM c WHERE (c.fullName.last = @param000_fullName__last) AND (c.id IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c.age = @param002_age)");

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

				"id", List.of("id001", "id002", "id005"), //
				"age >=", 30, //
				"fullName.last !=", "ABC") //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE (c.fullName.last = @param000_fullName__last) AND (c.id IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c.age >= @param002_age) AND (c.fullName.last != @param003_fullName__last) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

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

				"id", List.of("id001", "id002", "id005"), //
				"age", 30, //
				"fullName.first OR fullName.last STARTSWITH", "F", //
				"fullName.last CONTAINS", "L", //
				"skill ARRAY_CONTAINS", "Java") //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE (c.fullName.last = @param000_fullName__last) AND (c.id IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c.age = @param002_age) AND ( (STARTSWITH(c.fullName.first, @param003_fullName__first)) OR (STARTSWITH(c.fullName.last, @param004_fullName__last)) ) AND (CONTAINS(c.fullName.last, @param005_fullName__last)) AND (ARRAY_CONTAINS(c.skill, @param006_skill)) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

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

				"id", List.of("id001", "id002", "id005"), //
				"age", 30) //
				.fields("id", "fullName.first", "age") //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT VALUE {\"id\":c.id, \"fullName\":{\"first\":c.fullName.first}, \"age\":c.age} FROM c WHERE (c.fullName.last = @param000_fullName__last) AND (c.id IN (@param001_id__0, @param001_id__1, @param001_id__2)) AND (c.age = @param002_age) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

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
	public void buildQuerySpec_should_work_for_sub_cond() {

		var q = Condition.filter("fullName.last", "Hanks", //
				"SUB_COND_OR",
				List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")), //
				"age", 30) //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE (c.fullName.last = @param000_fullName__last) AND ((c.position = @param001_position) OR (c.organization = @param002_organization)) AND (c.age = @param003_age) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_fullName__last", "Hanks").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_position", "leader").toJson());
		assertThat(params.get(2).toJson()).isEqualTo(new SqlParameter("@param002_organization", "executive").toJson());
		assertThat(params.get(3).toJson()).isEqualTo(new SqlParameter("@param003_age", 30).toJson());
	}

	@Test
	public void buildQuerySpec_should_work_for_sub_cond_from_the_beginning() {

		var q = Condition.filter( //
				"SUB_COND_OR",
				List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
		) //
				.sort("_ts", "DESC") //
				.offset(10) //
				.limit(20) //
				.toQuerySpec();

		assertThat(q.getQueryText().trim()).isEqualTo(
				"SELECT * FROM c WHERE ((c.position = @param000_position) OR (c.organization = @param001_organization)) ORDER BY c._ts DESC OFFSET 10 LIMIT 20");

		var params = List.copyOf(q.getParameters());

		assertThat(params.get(0).toJson()).isEqualTo(new SqlParameter("@param000_position", "leader").toJson());
		assertThat(params.get(1).toJson()).isEqualTo(new SqlParameter("@param001_organization", "executive").toJson());
	}

}
