package io.github.thunderz99.cosmos.impl.mongo;

import java.util.*;
import java.util.stream.IntStream;

import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosBuilder;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.CosmosDatabaseTest;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoDatabaseImplTest {

    static Cosmos cosmos;
    static CosmosDatabase db;

    static String clusterName = "Databases";
    static String host = "UnitTest_Mongo_" + RandomStringUtils.randomAlphanumeric(4);

    static FullNameUser user1 = null;
    static FullNameUser user2 = null;
    static FullNameUser user3 = null;
    static FullNameUser user4 = null;

    Logger log = LoggerFactory.getLogger(MongoDatabaseImplTest.class);

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
        cosmos = new CosmosBuilder().withDatabaseType("mongodb").withConnectionString(EnvUtil.get("MONGODB_CONNECTION_STRING")).build();
        db = cosmos.createIfNotExist(host, "colls");

        initFamiliesData();
        initData4ComplexQuery();

    }

	@AfterAll
	public static void afterAll() throws Exception {
        deleteFamiliesData();
        deleteData4ComplexQuery();
        cosmos.deleteDatabase(host);
    }

    @Test
	void create_and_read_should_work() throws Exception {

		var user = new User("unittest_create_01", "first01", "last01");
        db.delete(host, user.id, "Users");

		try {
            var created = db.create(host, user, "Users").toObject(User.class);
			assertThat(created.id).isEqualTo(user.id);
			assertThat(created.firstName).isEqualTo(user.firstName);

            var read = db.read(host, user.id, "Users").toObject(User.class);
			assertThat(read.id).isEqualTo(user.id);
			assertThat(read.firstName).isEqualTo(user.firstName);

		} finally {
            db.delete(host, user.id, "Users");
		}

	}

    @Test
    void getId_should_work() {
        String testId = "getId_should_work_id";
        var user = new User(testId, "firstName", "lastName");

        // TODO: extract util class
        var id = MongoDatabaseImpl.getId(user);
        assertThat(id).isEqualTo(testId);

        id = MongoDatabaseImpl.getId(testId);
        assertThat(id).isEqualTo(testId);
    }



    @Test
    void checkValidId_should_work() {
        // normal check
        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("valid_id", "", ""));
            }

            // TODO: extract to util class
            MongoDatabaseImpl.checkValidId(testData);
        }

        {
            List<String> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add("valid_id");
            }

            MongoDatabaseImpl.checkValidId(testData);
        }

        // boundary checks
        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\t", "", ""));
            }

            assertThatThrownBy(() -> MongoDatabaseImpl.checkValidId(testData))
                    .hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\n", "", ""));
            }

            assertThatThrownBy(() -> MongoDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\r", "", ""));
            }

            assertThatThrownBy(() -> MongoDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }
    }

	@Test
	void create_should_throw_when_data_is_null() throws Exception {
		User user = null;
        assertThatThrownBy(() -> db.create(host, user, "Users")).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("create data UnitTest");

	}

	@Test
	void update_should_work() throws Exception {

		var user = new User("unittest_update_01", "first01", "last01");
        db.delete(host, user.id, "Users");

		try {
            db.create(host, user, "Users").toObject(User.class);

			var update1 = Map.of("lastName", "lastUpdated");
//			// partial update
//			var updated1 = db.updatePartial(coll, user.id, update1, "Users").toObject(User.class);
//			assertThat(updated1.id).isEqualTo(user.id);
//			assertThat(updated1.firstName).isEqualTo(user.firstName);
//			assertThat(updated1.lastName).isEqualTo(update1.get("lastName"));

			// full update
			user.firstName = "fullUpdateFirst";
			user.lastName = "fullUpdateLast";
            var updated2 = db.update(host, user, "Users").toObject(User.class);

			assertThat(updated2.id).isEqualTo(user.id);
			assertThat(updated2.firstName).isEqualTo(user.firstName);
			assertThat(updated2.lastName).isEqualTo(user.lastName);

		} finally {
            db.delete(host, user.id, "Users");
		}

	}

	@Test
	void upsert_should_work() throws Exception {
		var user = new User("unittest_upsert_01", "first01", "last01");
        db.delete(host, user.id, "Users");

		try {
            var upserted = db.upsert(host, user, "Users").toObject(User.class);
            assertThat(upserted.id).isEqualTo(user.id);
            assertThat(upserted.firstName).isEqualTo(user.firstName);

            var upsert1 = new User(user.id, "firstUpsert", "lastUpsert");

            // full upsert
            var upserted1 = db.upsert(host, upsert1, "Users").toObject(User.class);
            assertThat(upserted1.id).isEqualTo(upsert1.id);
            assertThat(upserted1.firstName).isEqualTo(upsert1.firstName);
            assertThat(upserted1.lastName).isEqualTo(upsert1.lastName);

        } finally {
            db.delete(host, user.id, "Users");
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
	void get_database_name_should_work() throws Exception {
        assertThat(db.getDatabaseName()).isEqualTo(host);
	}


    @Disabled
    void updatePartial_should_work() throws Exception {
        var partition = "SheetConents";

        var id = "updatePartial_should_work_001"; // form with content
        var age = 20;
        var formId = "829cc727-2d49-4d60-8f91-b30f50560af7"; //uuid
        var formContent = Map.of("name", "Tom", "sex", "Male", "address", "NY", "tags",
                List.of(Map.of("id", "t001", "name", "backend"), Map.of("id", "t002", "name", "frontend")));
        var data = Map.of("id", id, "age", age, formId, formContent, "sheet-2", Map.of("skills", Set.of("Java", "Python")));

        try {
            var upserted = db.upsert(host, data, partition).toMap();

            assertThat(upserted).containsKeys("id", "age", formId).doesNotContainKey("sort");

            {
                // normal update partial
                var partialMap = Map.of("name", "Jim", "sort", 99);
                var patched = db.updatePartial(host, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Jim")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                        .containsEntry("sort", 99).containsEntry("age", 20)
                ;
            }
            {
                // nested update partial
                var partialMap = Map.of("name", "Jane", "sheet-2", Map.of("skills", List.of("Java", "JavaScript")));
                var patched = db.updatePartial(host, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Jane")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);

                assertThat(((Map<String, Object>) patched.get("sheet-2")).get("skills")).isEqualTo(List.of("Java", "JavaScript"));

            }

            {
                // partial update containing fields more than 10
                var formMap = new HashMap<String, Integer>();
                IntStream.range(0, 10).forEach(i -> formMap.put("key" + i, i));
                var partialMap = Map.of("name", "Kate", formId, formMap);

                var patched = db.updatePartial(host, id, partialMap, partition).toMap();
                assertThat(patched).containsEntry("name", "Kate")
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);


                assertThat(((Map<String, Object>) patched.get(formId)).get("key5")).isEqualTo(5);

            }

        } finally {
            db.delete(host, id, partition);
        }

    }


    static void initFamiliesData() throws Exception {
        var partition = "Families";

        try (var is1 = CosmosDatabaseTest.class.getResourceAsStream("family1.json");
             var is2 = CosmosDatabaseTest.class.getResourceAsStream("family2.json")) {

            var family1 = JsonUtil.toMap(is1);
            var family2 = JsonUtil.toMap(is2);

            db.upsert(host, family1, partition);
            db.upsert(host, family2, partition);
        }

    }

    static void deleteFamiliesData() {
        cosmos.deleteCollection(host, "Families");
    }


    static void initData4ComplexQuery() throws Exception {
        user1 = new FullNameUser("id_find_filter1", "Elise", "Hanks", 12, "2020-10-01", "Blanco");
        user2 = new FullNameUser("id_find_filter2", "Matt", "Hanks", 30, "2020-11-01", "Typescript", "Javascript", "React", "Java");
        user3 = new FullNameUser("id_find_filter3", "Tom", "Henry", 45, "2020-12-01", "Java", "Go", "Python");
        user4 = new FullNameUser("id_find_filter4", "Andy", "Henry", 45, "2020-12-01", "Javascript", "Java");

        // prepare
        db.upsert(host, user1, "Users");
        db.upsert(host, user2, "Users");
        db.upsert(host, user3, "Users");
        // different partition
        db.upsert(host, user4, "Users2");
    }

    static void deleteData4ComplexQuery() throws Exception {
        if (db != null) {
            db.delete(host, user1.id, "Users");
            db.delete(host, user2.id, "Users");
            db.delete(host, user3.id, "Users");
            db.delete(host, user4.id, "Users2");
        }
    }

}
