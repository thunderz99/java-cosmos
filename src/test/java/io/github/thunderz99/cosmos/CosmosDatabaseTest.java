package io.github.thunderz99.cosmos;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.azure.documentdb.DocumentClientException;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.thunderz99.cosmos.util.JsonUtil;

class CosmosDatabaseTest {

	static Dotenv dotenv = Dotenv.load();
	static Cosmos cosmos;
	static CosmosDatabase db;

	static String dbName = "CosmosDB";
	static String coll = "UnitTest";

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
	public static void beforeAll() throws DocumentClientException {
		cosmos = new Cosmos(dotenv.get("COSMOSDB_CONNECTION_STRING"));
		db = cosmos.createIfNotExist(dbName, coll);

	}

	@AfterAll
	public static void afterAll() throws DocumentClientException {
		// cosmos.deleteCollection(dbName, coll);
		// cosmos.deleteDatabase(dbName);
	}

	@Test
	void createAndReadShouldWork() throws DocumentClientException {

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
	void createShouldThrowWhenDataNull() throws DocumentClientException {
		User user = null;
		assertThatThrownBy(() -> db.create(coll, user, "Users")).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("create data UnitTest Users");

	}

	@Test
	void updateShouldWork() throws DocumentClientException {

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
	void upsertShouldWork() throws DocumentClientException {
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

		public List<String> skills = new ArrayList<>();

		public FullNameUser() {
		}

		public FullNameUser(String id, String firstName, String lastName, int age, String... skills) {
			this.id = id;
			this.fullName = new FullName(firstName, lastName);
			this.age = age;
			if (skills != null) {
				this.skills.addAll(List.of(skills));
			}
		}

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

		public String toString(){
			return JsonUtil.toJson(this);
		}
	}

	@Test
	public void Find_should_work_with_filter() throws DocumentClientException {

		var user1 = new FullNameUser("id_find_filter1", "Elise", "Hanks", 12, "Blanco");
		var user2 = new FullNameUser("id_find_filter2", "Matt", "Hanks", 30, "Typescript", "Javascript", "React");
		var user3 = new FullNameUser("id_find_filter3", "Tom", "Henry", 45, "Java", "Go", "Python");

        try {
            // prepare
			db.upsert(coll, user1, "Users");
			db.upsert(coll, user2, "Users");
			db.upsert(coll, user3, "Users");

			// test basic find
            {
            	var cond = Condition.filter(
                        "fullName.last",  "Hanks", //
                        "fullName.first",  "Elise" //
                    )
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

				var users = db.find(
					coll,
					cond,
					"Users"
				).toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0)).hasToString(user1.toString());
			}

			// test fields
            {
            	var cond = Condition.filter(
                        "fullName.last",  "Hanks", //
                        "fullName.first",  "Elise" //
					)
					.fields("id", "fullName.last", "age")//
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

				var users = db.find(
					coll,
					cond,
					"Users"
				).toList(FullNameUser.class);

				assertThat(users.size()).isEqualTo(1);
				assertThat(users.get(0).id).isEqualTo(user1.id);
				assertThat(users.get(0).age).isEqualTo(user1.age);
				assertThat(users.get(0).fullName.last).isEqualTo(user1.fullName.last);
				assertThat(users.get(0).fullName.first).isNullOrEmpty();
				assertThat(users.get(0).skills).isEmpty();
            }

			// test IN find

            {
				var cond = Condition.filter(
                    "fullName.last",  "Hanks", //
                    "id", List.of(user1.id, user2.id, user3.id)
                )
	            .sort("_ts", "DESC") //
	            .limit(10) //
	            .offset(0);


                // test find
                var users = db.find(
                    coll,
                    cond,
                    "Users"
                ).toList(FullNameUser.class);

                assertThat(users.size()).isEqualTo(2);
				assertThat(users.get(0)).hasToString(user2.toString());

				//count

				var count = db.count(coll, cond, "Users");

				assertThat(count).isEqualTo(2);
            }

			// test limit find
			{
				var users = db.find(coll, Condition.filter().sort("_ts", "DESC").limit(2), "Users").toList(FullNameUser.class);
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

        } finally {
			db.delete(coll, user1.id, "Users");
			db.delete(coll, user2.id, "Users");
			db.delete(coll, user3.id, "Users");
        }
    }

}
