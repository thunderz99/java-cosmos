package io.github.thunderz99.cosmos;

import java.util.*;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CosmosDatabaseTest {

    static Cosmos cosmos;
    static CosmosDatabase db;

    static String dbName = "CosmosDB";
    static String coll = "UnitTest";

    static FullNameUser user1 = null;
    static FullNameUser user2 = null;
    static FullNameUser user3 = null;
    static FullNameUser user4 = null;

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
        cosmos = new Cosmos(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        db = cosmos.createIfNotExist(dbName, coll);

        initFamiliesData();
        initData4ComplexQuery();

    }

	@AfterAll
	public static void afterAll() throws Exception {
        deleteData4ComplexQuery();
    }

	@Test
	void create_and_read_should_work() throws Exception {

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
	void create_should_throw_when_data_is_null() throws Exception {
		User user = null;
		assertThatThrownBy(() -> db.create(coll, user, "Users")).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("create data UnitTest Users");

	}

	@Test
	void update_should_work() throws Exception {

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
	void upsert_should_work() throws Exception {
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
    public void find_should_work_with_filter() throws Exception {

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

        // test ARRAY_CONTAINS_ALL negative
        {
            var cond = Condition.filter( //
                    "skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java"), //
                    "age <", 100) //
                    .sort("id", "ASC") //
                    .not() //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(coll, cond, "Users");

            assertThat(count).isEqualTo(2);
        }

        // test ARRAY_CONTAINS_ALL negative using $NOT
        {
            var cond = Condition.filter( //
                    "$NOT", Map.of("$AND", //
                            List.of( //
                                    Map.of("skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java")), //
                                    Map.of("age <", 100)))) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(coll, cond, "Users");

            assertThat(count).isEqualTo(2);
        }

        // test ARRAY_CONTAINS_ALL negative using $NOT, simple version
        {
            var cond = Condition.filter( //
                    "$NOT", Map.of("skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java")), //
                    "age <", 100) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(coll, cond, "Users");

            assertThat(count).isEqualTo(2);
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

        // test aggregate(simple)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expect = Map.of("Hanks", 2, "Henry", 1);

            var last1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(expect.get(last1));

            var last2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(expect.get(last2));

        }

        // test aggregate(max)
        {
            var aggregate = Aggregate.function("MAX(c.age) AS maxAge, COUNT(1) AS facetCount").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expectAge = Map.of("Hanks", 30, "Henry", 45);
            var expectCount = Map.of("Hanks", 2, "Henry", 1);

            var last1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(Integer.parseInt(result.get(0).getOrDefault("maxAge", "-1").toString())).isEqualTo(expectAge.get(last1));
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(expectCount.get(last1));

            var last2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(Integer.parseInt(result.get(1).getOrDefault("maxAge", "-1").toString())).isEqualTo(expectAge.get(last2));
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(expectCount.get(last2));

        }

        // test aggregate(with order by)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("fullName.last");

            var cond = Condition.filter("age <", 100).sort("last", "DESC");

            // test find
            var result = db.aggregate(coll, aggregate, cond, "Users").toMap();
            assertThat(result).hasSize(2);

            var last1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(last1).isEqualTo("Henry");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);

            var last2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(last2).isEqualTo("Hanks");
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(2);

        }

        // test query cross-partition
        {
            // simple query
            var cond = Condition.filter("id LIKE", "id_find_filter%").sort("id", "ASC").crossPartition(true);
            var result = db.find(coll, cond).toList(FullNameUser.class);
            assertThat(result).hasSizeGreaterThanOrEqualTo(4);
            assertThat(result.get(0).id).isEqualTo("id_find_filter1");
            assertThat(result.get(3).id).isEqualTo("id_find_filter4");
        }

        // aggregate with cross-partition
        {
            var aggregate = Aggregate.function("COUNT(1) as facetCount").groupBy("_partition");
            var cond = Condition.filter("_partition", Set.of("Users", "Users2")).crossPartition(true);
            var result = db.aggregate(coll, aggregate, cond).toMap();
            assertThat(result).hasSize(2);
            assertThat(result.get(0).get("_partition")).isEqualTo("Users");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(3);
            assertThat(result.get(1).get("_partition")).isEqualTo("Users2");
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);

            System.out.println(result);

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

        {
            // using Condition.filter as a sub query
            var partition = "Families";

            var cond = Condition.filter(SubConditionType.OR, List.of( //
                    Condition.filter("address.state", "WA"), //
                    Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC") //
                    ;

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(2);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
            assertThat(items.get(1).get("creationDate")).hasToString("1431620462");
        }
        {
            // using map as a sub query (in order to support rest api 's parameter)
            var partition = "Families";

            var cond = Condition.filter(SubConditionType.OR, List.of( //
                    Map.of("address.state", "WA"), //
                    Map.of("id", "WakefieldFamily"))) //
                    .sort("id", "ASC") //
                    ;

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(2);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
            assertThat(items.get(1).get("creationDate")).hasToString("1431620462");
        }

        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(this.getClass().getResourceAsStream("familyQuery-OR.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(2);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
            assertThat(items.get(1).get("creationDate")).hasToString("1431620462");
        }

    }

	@Test
	void sub_cond_query_should_work_4_AND() throws Exception {
        // test json from cosmosdb official site
        // https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

        {
            // using Condition.filter as a sub query
            var partition = "Families";

            var cond = Condition.filter(SubConditionType.AND, List.of( //
                    Condition.filter("address.state", "WA"), //
                    Condition.filter("lastName", "Andersen"))) //
                    .sort("id", "ASC") //
                    ;

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using map as a sub query (in order to support rest api 's parameter)
            var partition = "Families";

            var cond = Condition.filter(SubConditionType.AND, List.of( //
                    Map.of("address.state", "WA"), //
                    Map.of("lastName", "Andersen"))) //
                    .sort("id", "ASC") //
                    ;

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(this.getClass().getResourceAsStream("familyQuery-AND.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(this.getClass().getResourceAsStream("familyQuery-AND2.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }

    }

    @Test
    void sub_cond_query_should_work_4_NOT() throws Exception {
        // test json from cosmosdb official site
        // https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(this.getClass().getResourceAsStream("familyQuery-NOT.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("WakefieldFamily");
        }

    }

    @Test
    void check_invalid_id_should_work() throws Exception {
        var ids = List.of("\ttabbefore", "tabafter\t", "tab\nbetween", "\ncrbefore", "crafter\r", "cr\n\rbetween");
        for (var id : ids) {
            assertThatThrownBy(() -> CosmosDatabase.checkValidId(id)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("id cannot contain");
        }

    }

    @Test
    void invalid_id_should_be_checked() throws Exception {

        var partition = "InvalidIdTest";
        var ids = List.of("\ttabbefore", "cr\rbetween");
        for (var id : ids) {
            try {
                var data = Map.of("id", id, "name", "Lee");
                assertThatThrownBy(() -> db.create(coll, data, partition).toMap()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(id);
                assertThatThrownBy(() -> db.upsert(coll, data, partition).toMap()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(id);
            } finally {
                var toDelete = db.find(coll, Condition.filter(), partition).toMap();
                for (var map : toDelete) {
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

	@Test
	void dynamic_field_with_hyphen_should_work() throws Exception {
        var partition = "SheetConents";

        var id = "D001"; // form with content
        var age = 20;
        var formId = "829cc727-2d49-4d60-8f91-b30f50560af7"; //uuid
        var formContent = Map.of("name", "Tom", "sex", "Male", "address", "NY");
        var data = Map.of("id", id, "age", age, formId, formContent, "sheet-2", Map.of("skills", Set.of("Java", "Python")));

        var id2 = "D002"; // form is empty
        var formContent2 = Map.of("name", "", "sex", "", "empty", true);
        var data2 = Map.of("id", id2, formId, formContent2);

        var id3 = "D003"; // form is undefined
        var data3 = Map.of("id", id3);


        try {
			db.upsert(coll, data, partition);
			db.upsert(coll, data2, partition);
			db.upsert(coll, data3, partition);

			{
				// dynamic fields
				var cond = Condition.filter("id", id, String.format("%s.name", formId), "Tom");
				var items = db.find(coll, cond, partition).toMap();

				assertThat(items).hasSize(1);
				var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
				assertThat(map).containsEntry("name", "Tom").containsEntry("sex", "Male").containsEntry("address", "NY");
            }

            {
                // IS_DEFINED = true
                var cond = Condition.filter("id", id, String.format("%s IS_DEFINED", formId), true);
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_DEFINED = false
                var cond = Condition.filter("id", id, "test IS_DEFINED", false);
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_NUMBER = true
                var cond = Condition.filter("id", id, "age IS_NUMBER", true);
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }
            {
                // IS_NUMBER = false
                var cond = Condition.filter("id", id, String.format("%s IS_NUMBER", formId), false);
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // use rawSql to implement IS_NUMBER
                var cond1 = Condition.filter("id", id);
                var cond2 = Condition.rawSql("IS_NUMBER(c.test) = false");
                var cond = Condition.filter(SubConditionType.AND, List.of(cond1, cond2));
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_DEFINED = false in OR condition. result: 2 item
                var cond = Condition.filter("id LIKE", "D00%", SubConditionType.OR, List.of(
                        Condition.filter(String.format("%s IS_DEFINED", formId), false),
                        Condition.filter(String.format("%s.empty", formId), true)
                )).sort("id", "ASC");
                var items = db.find(coll, cond, partition).toMap();
                assertThat(items).hasSize(2);
                assertThat(items.get(0).get("id")).isEqualTo(id2);
                assertThat(items.get(1).get("id")).isEqualTo(id3);
            }

            {
                // IS_DEFINED = false in OR condition. result: 1 item
                var cond = Condition.filter("id LIKE", "D00%", SubConditionType.AND, List.of(
                        Condition.filter(String.format("%s.name IS_DEFINED", formId), true),
                        Condition.filter(String.format("%s.empty IS_DEFINED", formId), false)
                )).sort("id", "ASC");
                var items = db.find(coll, cond, partition).toMap();
				assertThat(items).hasSize(1);
				assertThat(items.get(0).get("id")).isEqualTo(id);
			}

			{
                // nested fields
                var cond = Condition.filter("id", id, String.format("%s.name", formId), "Tom").fields("id", String.format("%s.name", formId), String.format("%s.sex", formId), "sheet-2.skills");
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Tom").containsEntry("sex", "Male").doesNotContainEntry("address", "NY");

                var map2 = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get("sheet-2")));
                assertThat(map2).containsKey("skills");
                assertThat(map2.values().toString()).contains("Java", "Python");


            }


        } finally {
            db.delete(coll, id, partition);
            db.delete(coll, id2, partition);
            db.delete(coll, id3, partition);
        }

    }

    @Test
    void updatePartial_should_work() throws Exception {
        var partition = "SheetConents";

        var id = "updatePartial_should_work_001"; // form with content
        var age = 20;
        var formId = "829cc727-2d49-4d60-8f91-b30f50560af7"; //uuid
        var formContent = Map.of("name", "Tom", "sex", "Male", "address", "NY", "tags",
                List.of(Map.of("id", "t001", "name", "backend"), Map.of("id", "t002", "name", "frontend")));
        var data = Map.of("id", id, "age", age, formId, formContent, "sheet-2", Map.of("skills", Set.of("Java", "Python")));

        try {
            var upserted = db.upsert(coll, data, partition).toMap();

            assertThat(upserted).containsKeys("id", "age", formId).doesNotContainKey("sort");

            {
                // normal update partial
                var partialMap = Map.of("name", "Jim", "sort", 99);
                var patched = db.updatePartial(coll, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Jim")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                        .containsEntry("sort", 99).containsEntry("age", 20)
                ;
            }
            {
                // nested update partial
                var partialMap = Map.of("name", "Jane", "sheet-2", Map.of("skills", List.of("Java", "JavaScript")));
                var patched = db.updatePartial(coll, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Jane")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);

                assertThat(((Map<String, Object>) patched.get("sheet-2")).get("skills")).isEqualTo(List.of("Java", "JavaScript"));

            }

            {
                // partial update an array's item
                if (Boolean.parseBoolean(EnvUtil.getOrDefault(Cosmos.COSMOS_SDK_V4_ENABLE, "false"))) {
                    // only supported wen SDK v4 is enabled
                    var partialMap = Map.of("name", "Alex", "sheet-2", Map.of("skills/1", "Kotlin"));

                    var patched = db.updatePartial(coll, id, partialMap, partition).toMap();
                    assertThat(patched).containsEntry("name", "Alex")
                            .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                            .containsEntry("_partition", partition);

                    assertThat(((Map<String, Object>) patched.get("sheet-2")).get("skills")).isEqualTo(List.of("Java", "Kotlin"));
                }
            }

            {
                // partial update an array's nested item
                if (Boolean.parseBoolean(EnvUtil.getOrDefault(Cosmos.COSMOS_SDK_V4_ENABLE, "false"))) {
                    // only supported wen SDK v4 is enabl
                    var partialMap = Map.of("name", "Kate", formId, Map.of("tags/0", Map.of("name", "fullstack")));

                    var patched = db.updatePartial(coll, id, partialMap, partition).toMap();
                    assertThat(patched).containsEntry("name", "Kate")
                            .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                            .containsEntry("_partition", partition);

                    assertThat(((List<Map<String, Object>>) ((Map<String, Object>) patched.get(formId)).get("tags")).get(0).get("name")).isEqualTo("fullstack");
                }
            }
            {
                // partial update containing fields more than 10
                var formMap = new HashMap<String, Integer>();
                IntStream.range(0, 10).forEach(i -> formMap.put("key" + i, i));
                var partialMap = Map.of("name", "Kate", formId, formMap);

                var patched = db.updatePartial(coll, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Kate")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);


                assertThat(((Map<String, Object>) patched.get(formId)).get("key5")).isEqualTo(5);

            }

        } finally {
            db.delete(coll, id, partition);
        }

    }

    @Test
    void updatePartial_should_work_with_optimistic_concurrency_control() throws Exception {
        var partition = "SheetConents";

        var id = "updatePartial_should_work_with_optimistic_concurrency_control"; // form with content
        var age = 20;
        var formId = "03a69e73-18b8-44e5-a0b1-1b2739ca6e60"; //uuid
        var formContent = Map.of("name", "Tom", "sex", "Male", "address", "NY", "tags",
                List.of(Map.of("id", "t001", "name", "backend"), Map.of("id", "t002", "name", "frontend")));
        var data = Map.of("id", id, "age", age, "sort", "010", formId, formContent, "sheet-2", Map.of("skills", Set.of("Java", "Python")));

        try {
            var upserted = db.upsert(coll, data, partition).toMap();

            assertThat(upserted).containsKeys("id", "age", formId, "_etag");

            {
                // read by A,B, update by B and A with different field should succeed
                // PartialUpdateOption.checkETag is false(default)

                var partialMapA = Map.of("age", 25);

                var partialMapB = Map.of("sort", "099", "employeeCode", "X0123");

                var patchedB = db.updatePartial(coll, id, partialMapB, partition).toMap();

                var patchedA = db.updatePartial(coll, id, partialMapA, partition).toMap();


                // B should update sort and add employeeCode only
                assertThat(patchedB).containsEntry("sort", "099").containsEntry("employeeCode", "X0123")
                        .containsEntry("age", 20) // age keep not change
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                ;

                // A should update age only, and retain B's sort value
                assertThat(patchedA).containsEntry("age", 25) // age updated
                        .containsEntry("sort", "099").containsEntry("employeeCode", "X0123") // sort and employeeCode remains B's result
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                ;

                var partialMapC = Map.of("city", "Tokyo", Cosmos.ETAG, "invalid etag");

                // etag will be ignored, if PartialUpdateOption.checkETag false. So the following operation should succeed.
                var patchedC = db.updatePartial(coll, id, partialMapC, partition).toMap();

                assertThat(patchedC).containsEntry("city", "Tokyo").containsEntry("age", 25).containsEntry("sort", "099");

            }

            {
                // read by A,B, update by B and A with different field should succeed
                // PartialUpdateOption.checkETag is true

                var originData = db.read(coll, id, partition).toMap();

                var etag = originData.getOrDefault("_etag", "").toString();
                assertThat(etag).isNotEmpty();

                Map<String, Object> partialMapA = Maps.newHashMap(Map.of("age", 30, Cosmos.ETAG, etag));

                Map<String, Object> partialMapB = Map.of("sort", "199", "employeeCode", "X0456", Cosmos.ETAG, etag);

                var patchedB = db.updatePartial(coll, id, partialMapB, partition, PartialUpdateOption.checkETag(true)).toMap();

                // B should update sort and add employeeCode only
                assertThat(patchedB).containsEntry("sort", "199").containsEntry("employeeCode", "X0456")
                        .containsEntry("age", 25) // age keep not change
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                ;

                // new etag should be generated
                assertThat(patchedB.getOrDefault(Cosmos.ETAG, "").toString()).isNotEmpty().isNotEqualTo(etag);


                assertThatThrownBy(() -> db.updatePartial(coll, id, partialMapA, partition, PartialUpdateOption.checkETag(true)).toMap())
                        .isInstanceOfSatisfying(CosmosException.class, (e) -> {
                            assertThat(e.getStatusCode()).isEqualTo(412);
                        });

                // empty etag will be ignored
                partialMapA.put(Cosmos.ETAG, "");
                var patchedA = db.updatePartial(coll, id, partialMapA, partition, PartialUpdateOption.checkETag(true)).toMap();

                // the result should be correct partial updated
                assertThat(patchedA).containsEntry("age", 30) // age updated
                        .containsEntry("sort", "199").containsEntry("employeeCode", "X0456") // sort and employeeCode remains B's result
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);


            }

        } finally {
            db.delete(coll, id, partition);
        }

    }

    @Test
    void dynamic_field_should_work_for_ARRAY_CONTAINS_ALL() throws Exception {
        var partition = "SheetConents2";

        var id = "dynamic_field_should_work_for_ARRAY_CONTAINS_ALL"; // form with content
        var formId = "421f118a-543e-49e9-88c1-dba77b7f990f"; //uuid
        var formContent = Map.of("name", "Jerry", "value", Set.of("Java", "Typescript", "Python"));
        var data = Map.of("id", id, formId, formContent);


        try {
            db.upsert(coll, data, partition);

            {
                // dynamic fields with ARRAY_CONTAINS_ALL
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), Set.of("Java", "Python"));
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }
            {
                // dynamic fields with ARRAY_CONTAINS_ALL, not hit
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), Set.of("Java", "CSharp"));
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }
            {
                // dynamic fields with ARRAY_CONTAINS_ANY
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ANY", formId), Set.of("Java", "CSharp"));
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }

            {
                // dynamic fields with ARRAY_CONTAINS
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS", formId), "Java");
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }

        } finally {
            db.delete(coll, id, partition);
        }

    }

    @Test
    void number_should_be_read_write_correctly() throws Exception {
        var partition = "NumberTest";
        var prefix = "number_should_be_read_write_correctly";

        var id1 = prefix + "integer";
        var id2 = prefix + "double";

        var data1 = Map.of("id", id1, "contents", Map.of("age", 20));
        var data2 = Map.of("id", id2, "contents", Map.of("age", 40.0));

        try {
            {
                // integer should be upserted as an integer and read as an integer
                var upserted1 = db.upsert(coll, data1, partition).toMap();
                assertThat((Map<String, Object>) upserted1.get("contents")).containsEntry("age", 20);
                var read1 = db.read(coll, id1, partition).toMap();
                assertThat((Map<String, Object>) read1.get("contents")).containsEntry("age", 20);
            }

            {
                // double should be upserted as a double and read as a double
                var upserted2 = db.upsert(coll, data2, partition).toMap();

                // At present, v2 sdk should read this value as an integer. When we up to v4 sdk. This problem will be resolved.
                assertThat((Map<String, Object>) upserted2.get("contents")).containsEntry("age", 40);
                var read2 = db.read(coll, id2, partition).toMap();
                assertThat((Map<String, Object>) read2.get("contents")).containsEntry("age", 40);
            }


        } finally {
            db.delete(coll, id1, partition);
            db.delete(coll, id2, partition);
        }


    }

    @Test
    void increment_should_work() throws Exception {
        var partition = "IncrementTests";
        var id = "increment_should_work";


        try {
            var data1 = Map.of("id", id, "name", "John", "contents", Map.of("age", 20), "score", 85.5);
            db.upsert(coll, data1, partition).toMap();
            {
                // increment by 1
                var inc1 = db.increment(coll, id, "/contents/age", 1, partition).toMap();
                assertThat((Map<String, Object>) inc1.get("contents")).containsEntry("age", 21);

                // increment by -3
                var inc2 = db.increment(coll, id, "/contents/age", -3, partition).toMap();
                assertThat((Map<String, Object>) inc2.get("contents")).containsEntry("age", 18);
            }

            {
                // failed when incrementing a string field
                assertThatThrownBy(() -> {
                    db.increment(coll, id, "name", 5, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("inputs is invalid");
                });
            }

            {
                // failed when incrementing a double field
                assertThatThrownBy(() -> {
                    db.increment(coll, id, "score", 5, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("inputs is invalid");
                });
            }

            db.increment(coll, id, "score", 5, partition);


        } finally {
            db.delete(coll, id, partition);
        }

    }

    @Test
    void merge_should_work_for_nested_json() {

        var map1 = new LinkedHashMap<String, Object>();
        map1.put("id", "ID001");
        map1.put("name", "Tom");
        map1.put("sort", "010");
        var contents = new LinkedHashMap<String, Object>();
        contents.put("phone", "12345");
        contents.put("addresses", Lists.newArrayList("NY", "DC"));
        map1.put("contents", contents);

        var map2 = new LinkedHashMap<String, Object>();
        map2.put("name", "Jane");
        var contents2 = new LinkedHashMap<String, Object>();
        contents2.put("skill", "backend");
        contents.put("addresses", Lists.newArrayList("NY", "Houston"));
        map2.put("contents", contents2);

        var merged = CosmosDatabase.merge(map1, map2);

        assertThat(merged).containsEntry("name", "Jane") // updated
                .containsEntry("sort", "010") // reserved
        ;
        assertThat((Map<String, Object>) merged.get("contents"))
                .containsEntry("phone", "12345") // reserved
                .containsEntry("skill", "backend") //updated
                .containsEntry("addresses", Lists.newArrayList("NY", "Houston")) //updated
        ;

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

    static void initData4ComplexQuery() throws Exception {
        user1 = new FullNameUser("id_find_filter1", "Elise", "Hanks", 12, "2020-10-01", "Blanco");
        user2 = new FullNameUser("id_find_filter2", "Matt", "Hanks", 30, "2020-11-01", "Typescript", "Javascript", "React", "Java");
        user3 = new FullNameUser("id_find_filter3", "Tom", "Henry", 45, "2020-12-01", "Java", "Go", "Python");
        user4 = new FullNameUser("id_find_filter4", "Andy", "Henry", 45, "2020-12-01", "Javascript", "Java");

        // prepare
        db.upsert(coll, user1, "Users");
        db.upsert(coll, user2, "Users");
        db.upsert(coll, user3, "Users");
        // different partition
        db.upsert(coll, user4, "Users2");
    }

    static void deleteData4ComplexQuery() throws Exception {
        if (db != null) {
            db.delete(coll, user1.id, "Users");
            db.delete(coll, user2.id, "Users");
            db.delete(coll, user3.id, "Users");
            db.delete(coll, user4.id, "Users2");
        }
    }

}
