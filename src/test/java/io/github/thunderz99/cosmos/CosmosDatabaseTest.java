package io.github.thunderz99.cosmos;

import static org.assertj.core.api.Assertions.*;
import static io.github.thunderz99.cosmos.condition.Condition.SubConditionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class CosmosDatabaseTest {

	static Dotenv dotenv = Dotenv.load();
	static Cosmos cosmos;
	static CosmosDatabase db;

	static String dbName = "CosmosDB";
	static String coll = "UnitTest";

	Logger log = LoggerFactory.getLogger(CosmosDatabaseTest.class);

	public static class User {
		public String id;
		public String firstName;
		public String lastName;

		public User() {
		}

		public User(String id, String firstName, String lastName) {
			this.id = id;
			this.firstName = firstName;
			this.lastName = lastName;
		}
	}

	@BeforeAll
	public static void beforeAll() throws Exception {
		cosmos = new Cosmos(dotenv.get("COSMOSDB_CONNECTION_STRING"));
		db = cosmos.createIfNotExist(dbName, coll);

		initFamiliesData();

	}

	@AfterAll
	public static void afterAll() throws Exception {
		// cosmos.deleteCollection(dbName, coll);
		// cosmos.deleteDatabase(dbName);
	}

	@Test
	void createAndReadShouldWork() throws Exception {

		var user = new User("unittest_create_01", "first01", "last01");
		db.delete(coll, user.id, "Users");

		try {
			var created = db.create(coll, user, "Users").toObject(User.class);
			assertThat(created.id).isEqualTo(user.id);
			assertThat(created.firstName).isEqualTo(user.firstName);

			var read = db.read(coll, user.id, "Users").toObject(User.class);
			assertThat(read.id).isEqualTo(user.id);
			assertThat(read.firstName).isEqualTo(user.firstName);

		} finally {
			db.delete(coll, user.id, "Users");
		}

	}

	@Test
	void createShouldThrowWhenDataNull() throws Exception {
		User user = null;
		assertThatThrownBy(() -> db.create(coll, user, "Users")).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("create data UnitTest Users");

	}

	@Test
	void updateShouldWork() throws Exception {

		var user = new User("unittest_update_01", "first01", "last01");
		db.delete(coll, user.id, "Users");

		try {
			db.create(coll, user, "Users").toObject(User.class);

			var update1 = Map.of("lastName", "lastUpdated");
			// partial update
			var updated1 = db.updatePartial(coll, user.id, update1, "Users").toObject(User.class);
			assertThat(updated1.id).isEqualTo(user.id);
			assertThat(updated1.firstName).isEqualTo(user.firstName);
			assertThat(updated1.lastName).isEqualTo(update1.get("lastName"));

			// full update
			user.firstName = "fullUpdateFirst";
			user.lastName = "fullUpdateLast";
			var updated2 = db.update(coll, user, "Users").toObject(User.class);

			assertThat(updated2.id).isEqualTo(user.id);
			assertThat(updated2.firstName).isEqualTo(user.firstName);
			assertThat(updated2.lastName).isEqualTo(user.lastName);

		} finally {
			db.delete(coll, user.id, "Users");
		}

	}

	@Test
	void upsertShouldWork() throws Exception {
		var user = new User("unittest_upsert_01", "first01", "last01");
		db.delete(coll, user.id, "Users");

		try {
			var upserted = db.upsert(coll, user, "Users").toObject(User.class);
			assertThat(upserted.id).isEqualTo(user.id);
			assertThat(upserted.firstName).isEqualTo(user.firstName);

			var upsert1 = new User(user.id, "firstUpsert", "lastUpsert");

			// full upsert
			var upserted1 = db.upsert(coll, upsert1, "Users").toObject(User.class);
			assertThat(upserted1.id).isEqualTo(upsert1.id);
			assertThat(upserted1.firstName).isEqualTo(upsert1.firstName);
			assertThat(upserted1.lastName).isEqualTo(upsert1.lastName);

			// partial upsert
			var upsertedParial = db.upsertPartial(coll, user.id, Map.of("lastName", "lastPartialUpsert"), "Users")
					.toObject(User.class);
			assertThat(upsertedParial.id).isEqualTo(upsert1.id);
			assertThat(upsertedParial.firstName).isEqualTo(upsert1.firstName);
			assertThat(upsertedParial.lastName).isEqualTo("lastPartialUpsert");

		} finally {
			db.delete(coll, user.id, "Users");
		}

	}

	public static class FullNameUser {
		public String id;

		public FullName fullName;

		public int age;

		/**
		 * a reserved word in cosmosdb ( to test c["end"])
		 */
		public String end;

		public List<String> skills = new ArrayList<>();

		public FullNameUser() {
		}

		public FullNameUser(String id, String firstName, String lastName, int age, String end, String... skills) {
			this.id = id;
			this.fullName = new FullName(firstName, lastName);
			this.age = age;
			this.end = end;
			if (skills != null) {
				this.skills.addAll(List.of(skills));
			}
		}

		@Override
		public String toString() {
			return JsonUtil.toJson(this);
		}
	}

	public static class FullName {
		public String first;
		public String last;

		public FullName() {
		}

		public FullName(String first, String last) {
			this.first = first;
			this.last = last;
		}

		@Override
		public String toString() {
			return JsonUtil.toJson(this);
		}
	}

	public enum Skill {
		Typescript, Javascript, Java, Python, Go
	}

	@Test
	public void Find_should_work_with_filter() throws Exception {

		var user1 = new FullNameUser("id_find_filter1", "Elise", "Hanks", 12, "2020-10-01", "Blanco");
		var user2 = new FullNameUser("id_find_filter2", "Matt", "Hanks", 30, "2020-11-01", "Typescript", "Javascript", "React", "Java");
		var user3 = new FullNameUser("id_find_filter3", "Tom", "Henry", 45, "2020-12-01", "Java", "Go", "Python");

		try {
			// prepare
			db.upsert(coll, user1, "Users");
			db.upsert(coll, user2, "Users");
			db.upsert(coll, user3, "Users");

			// test basic find
			{
				var cond = Condition.filter("fullName.last", "Hanks", //
						"fullName.first", "Elise" //
				).sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0)).hasToString(user1.toString());
			}

			// test reserved word find("end")
			{
				var cond = Condition.filter("fullName.last", "Hanks", //
						"end >=", "2020-10-30" //
				).sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0)).hasToString(user2.toString());
			}

			// test fields
			{
				var cond = Condition.filter("fullName.last", "Hanks", //
						"fullName.first", "Elise" //
				).fields("id", "fullName.last", "age")//
						.sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0).id).isEqualTo(user1.id);
				assertThat(users.get(0).age).isEqualTo(user1.age);
				assertThat(users.get(0).fullName.last).isEqualTo(user1.fullName.last);
				assertThat(users.get(0).fullName.first).isNullOrEmpty();
				assertThat(users.get(0).skills).isEmpty();
			}

			// test IN find

			{
				var cond = Condition.filter("fullName.last", "Hanks", //
						"id", List.of(user1.id, user2.id, user3.id)).sort("_ts", "DESC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(2);
				assertThat(users.get(0)).hasToString(user2.toString());

				// count

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(2);
			}

			// test limit find
			{
				var users = db.find(coll, Condition.filter().sort("_ts", "DESC").limit(2), "Users")
						.toList(FullNameUser.class);
				assertThat(users.size()).isEqualTo(2);
				assertThat(users.get(0)).hasToString(user3.toString());

				var maps = db.find(coll, Condition.filter().sort("_ts", "DESC").limit(2), "Users").toMap();
				assertThat(maps.size()).isEqualTo(2);
				assertThat(maps.get(1).get("id")).hasToString(user2.id);
				assertThat(maps.get(1).get("fullName").toString()).contains(user2.fullName.first);
			}

			// test compare operator
			{
				var cond = Condition.filter( //
						"fullName.last !=", "Henry", //
						"age >=", 30).sort("_ts", "DESC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0)).hasToString(user2.toString());

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(1);
			}

			// test function operator
			{
				var cond = Condition.filter( //
						"fullName.last STARTSWITH", "Ha", //
						"age <", 45, //
						"fullName.first CONTAINS", "at", //
						"skills ARRAY_CONTAINS", "Typescript")//
						.sort("_ts", "DESC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0)).hasToString(user2.toString());

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(1);
			}
			// test LIKE
			{
				var cond = Condition.filter( //
						"fullName.last LIKE", "_ank_", //
						"age <", 100) //
						.sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(2);
				assertThat(users.get(1).id).isEqualTo(user2.id);

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(2);
			}

			// test ARRAY_CONTAINS_ANY
			{
				var cond = Condition.filter( //
						"skills ARRAY_CONTAINS_ANY", List.of("Typescript", "Blanco"), //
						"age <", 100) //
						.sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(2);
				assertThat(users.get(0).id).isEqualTo(user1.id);
				assertThat(users.get(1).id).isEqualTo(user2.id);

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(2);
			}
			// test ARRAY_CONTAINS_ALL
			{
				var cond = Condition.filter( //
						"skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java"), //
						"age <", 100) //
						.sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0).id).isEqualTo(user2.id);

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(1);
			}

			// test enum
			{
				var cond = Condition.filter( //
						"skills ARRAY_CONTAINS", Skill.Python, //
						"age <", 100) //
						.sort("id", "ASC") //
						.limit(10) //
						.offset(0);

				// test find
				var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0).id).isEqualTo(user3.id);
			}

		} finally {
			db.delete(coll, user1.id, "Users");
			db.delete(coll, user2.id, "Users");
			db.delete(coll, user3.id, "Users");
		}
	}

	@Test
	void raw_query_spec_should_work() throws Exception {
		// test json from cosmosdb official site
		// https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

		var partition = "Families";

		var queryText = "SELECT c.gender, c.grade\n" + "    FROM Families f\n"
				+ "    JOIN c IN f.children WHERE f.address.state = @state ORDER BY f.id ASC";

		var params = new SqlParameterCollection(new SqlParameter("@state", "NY"));

		var cond = Condition.rawSql(queryText, params);

		var children = db.find(coll, cond, partition).toMap();

		assertThat(children).hasSize(2);

		assertThat(children.get(0).get("gender")).hasToString("female");
		assertThat(children.get(1).get("grade")).hasToString("8");

	}

	@Test
	void sub_cond_query_should_work_4_OR() throws Exception {
		// test json from cosmosdb official site
		// https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

		var partition = "Families";

		var cond = Condition.filter(SubConditionType.SUB_COND_OR, List.of( //
				Condition.filter("address.state", "WA"), //
				Condition.filter("id", "WakefieldFamily"))) //
				.sort("id", "ASC") //
				;

		var items = db.find(coll, cond, partition).toMap();

		assertThat(items).hasSize(2);

		assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
		assertThat(items.get(1).get("creationDate")).hasToString("1431620462");

	}

	@Test
	void sub_cond_query_should_work_4_AND() throws Exception {
		// test json from cosmosdb official site
		// https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

		var partition = "Families";

		var cond = Condition.filter(SubConditionType.SUB_COND_AND, List.of( //
				Condition.filter("address.state", "WA"), //
				Condition.filter("lastName", "Andersen"))) //
				.sort("id", "ASC") //
				;

		var items = db.find(coll, cond, partition).toMap();

		assertThat(items).hasSize(1);

		assertThat(items.get(0).get("id")).hasToString("AndersenFamily");

	}

	@Test
	void check_invalid_id_should_work() throws Exception {
		var ids = Lists.newArrayList("\ttabbefore", "tabafter\t", "tab\nbetween", "\ncrbefore", "crafter\r", "cr\n\rbetween");
		for(var id: ids){
			assertThatThrownBy( () -> CosmosDatabase.checkValidId(id)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("id cannot contain");
		}

	}
	@Test
	void invalid_id_should_be_checked() throws Exception {

		var partition = "InvalidIdTest";
		var ids = Lists.newArrayList("\ttabbefore", "cr\rbetween");
		for(var id: ids) {
			try {
				var data = Map.of("id", id, "name", "Lee");
				assertThatThrownBy( () -> db.create(coll, data, partition).toMap()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(id);
				assertThatThrownBy( () -> db.upsert(coll, data, partition).toMap()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(id);
			} finally {
				var toDelete = db.find(coll, Condition.filter(), partition).toMap();
				for(var map : toDelete){
					var selfLink = map.get("_self").toString();
					db.deleteBySelfLink(selfLink, partition);
				}
			}
		}

	}

	@Test
	void delete_by_self_link_should_work() throws Exception {

		var partition = "SelfLink";
		var id = "delete_by_self_link_should_work";
		try {
			var data = Map.of("id", id, "name", "Lee");
			var upserted = db.upsert(coll, data, partition).toMap();
			assertThat(upserted).isNotNull();

			var selfLink = upserted.get("_self").toString();
			assertThat(selfLink).isNotNull();

			db.deleteBySelfLink(selfLink, partition);

			var read = db.readSuppressing404(coll, id, partition);

			//not exist after deleteBySelfLink
			assertThat(read).isNull();

		} finally {
			db.delete(coll, id, partition);
		}
	}

	@Test
	void get_database_name_should_work() throws Exception {
		assertThat(db.getDatabaseName()).isEqualTo(dbName);
	}

	static void initFamiliesData() throws Exception {
		var partition = "Families";

		try (var is1 = CosmosDatabaseTest.class.getResourceAsStream("family1.json");
			 var is2 = CosmosDatabaseTest.class.getResourceAsStream("family2.json")) {

			var family1 = JsonUtil.toMap(is1);
			var family2 = JsonUtil.toMap(is2);

			db.upsert(coll, family1, partition);
			db.upsert(coll, family2, partition);
		}

	}

}
