package io.github.thunderz99.cosmos.impl.mongo;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.*;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.CheckBox;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

class MongoDatabaseImplTest {

    static Cosmos cosmos;
    static CosmosDatabase db;

    static String clusterName = "Databases";

    /**
     * Temporary database name for unit test, which will be created and deleted on the fly
     */
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
        cosmos = new CosmosBuilder().withDatabaseType("mongodb")
                .withConnectionString(EnvUtil.getOrDefault("MONGODB_CONNECTION_STRING", MongoImplTest.LOCAL_CONNECTION_STRING))
                .build();
        // we do not need to create a collection here, so the second param is empty
        // we create collections in specific test cases
        db = cosmos.createIfNotExist(host, "");
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
            // create
            var created = db.create(host, user, "Users").toObject(User.class);
            assertThat(created.id).isEqualTo(user.id);
            assertThat(created.firstName).isEqualTo(user.firstName);

            // read
            var read = db.read(host, user.id, "Users").toObject(User.class);
            assertThat(read.id).isEqualTo(user.id);
            assertThat(read.firstName).isEqualTo(user.firstName);

            // check _ts exist for timestamp
            var map = db.read(host, user.id, "Users").toMap();
            assertThat((Long) map.get("_ts")).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0));

            // read not existing document should throw CosmosException(404 Not Found)
            assertThatThrownBy(() -> db.read(host, "notExistId", "Users"))
                    .isInstanceOfSatisfying(CosmosException.class, ce -> {
                        assertThat(ce.getStatusCode()).isEqualTo(404);
                        assertThat(ce.getMessage()).contains("NotFound").contains("Resource Not Found");
                    });

            // readSuppressing404 should return null
            assertThat(db.readSuppressing404(host, "notExistId2", "Users")).isNull();

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
            // partial update
            var updated1 = db.updatePartial(host, user.id, update1, "Users").toObject(User.class);
            assertThat(updated1.id).isEqualTo(user.id);
            assertThat(updated1.firstName).isEqualTo(user.firstName);
            assertThat(updated1.lastName).isEqualTo(update1.get("lastName"));

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

            // _ts exist for timestamp
            var map = db.read(host, user.id, "Users").toMap();
            var timestamp1 = (Long) map.get("_ts");
            assertThat(timestamp1).isNotNull().isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0));


            var upsert1 = new User(user.id, "firstUpsert", "lastUpsert");

            // full upsert
            Thread.sleep(1000);
            var upserted1 = db.upsert(host, upsert1, "Users").toObject(User.class);
            assertThat(upserted1.id).isEqualTo(upsert1.id);
            assertThat(upserted1.firstName).isEqualTo(upsert1.firstName);
            assertThat(upserted1.lastName).isEqualTo(upsert1.lastName);

            // _ts should also be updated
            var upserted1Map = db.read(host, upsert1.id, "Users").toMap();
            var timestamp2 = (Long) upserted1Map.get("_ts");
            assertThat(timestamp2).isNotNull().isGreaterThan(timestamp1)
                    .isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0));


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

    @Test
    void doCheckBeforeBatch_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            MongoDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBatch(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBatch(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
        }

        {
            // number of operations exceed the limit should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 101; i++) {
                testData.add(new User());
            }

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("The number of data operations should not exceed 100.");
        }
    }

    @Test
    void doCheckBeforeBulk_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            MongoDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBulk(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> MongoDatabaseImpl.doCheckBeforeBulk(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
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

    @Test
    public void find_should_work_with_filter() throws Exception {

        // test basic find
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "fullName.first", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user1.toString());
        }

        // test basic find using OR
        {
            var cond = Condition.filter("fullName.first OR fullName.last", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

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

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

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

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

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
                            "id", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0)).hasToString(user2.toString());

            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test limit find
        {
            var users = db.find(host, Condition.filter().sort("id", "DESC").limit(2), "Users")
                    .toList(FullNameUser.class);
            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0)).hasToString(user3.toString());

            var maps = db.find(host, Condition.filter().sort("id", "DESC").limit(2), "Users").toMap();
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user2.toString());

            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(1);
        }

        // test function operator
        {
            var cond = Condition.filter( //
                            "fullName.last STARTSWITH", "Ha", //
                            "age <", 45, //
                            "fullName.first CONTAINS", "at", //
                            "skills ARRAY_CONTAINS", "Typescript"//
                    )
                    .sort("id", "DESC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user2.toString());

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(1).id).isEqualTo(user2.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user2.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0).id).isEqualTo(user2.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users");
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
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0).id).isEqualTo(user3.id);
        }
    }

    @Test
    void find_should_work_for_array_contains_any_field_query() throws Exception {
        var partition = "Families";

        {
            // ARRAY_CONTAINS_ANY
            // children is an array, and grade is a field of children
            var cond = Condition.filter("children ARRAY_CONTAINS_ANY grade", List.of(5, 8));
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(2);
        }

        {
            // ARRAY_CONTAINS_ALL
            // children is an array, and grade is a field of children
            var cond = Condition.filter("children ARRAY_CONTAINS_ALL grade", List.of(1, 8));
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(1);
        }
    }

    @Test
    void find_with_true_false_condition_should_work() throws Exception {
        var partition = "Families";

        {
            // false condition AND
            var cond = Condition.filter("lastName", "Andersen", "$AND and_test", Condition.falseCondition());
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(0);
        }
        {
            // false condition OR
            var cond = Condition.filter("$OR", List.of(
                    Condition.filter("lastName", "Andersen"),
                    Condition.falseCondition())
            );
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(1);
        }

        {
            // true condition AND
            var cond = Condition.filter("lastName", "Andersen", "$AND and_test", Condition.trueCondition());
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(1);

        }

        {
            // true condition OR
            var cond = Condition.filter("$OR", List.of(
                    Condition.filter("lastName", "Andersen"),
                    Condition.trueCondition())
            );
            var docs = db.find(host, cond, partition).toMap();
            // all data in partition
            assertThat(docs).hasSizeGreaterThanOrEqualTo(2);

        }
    }


    @Test
    public void fields_with_empty_field_should_work() throws Exception {
        // test fields with fields ["id", ""]
        {
            // empty field should be ignored
            var cond = Condition.filter().fields("id", "");

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isGreaterThanOrEqualTo(3);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(0).age).isEqualTo(0);
            assertThat(users.get(0).end).isNull();
            assertThat(users.get(0).skills).isEmpty();
        }
    }

    @Test
    public void regex_should_work_with_filter() throws Exception {

        // test regex match
        {
            var cond = Condition.filter("fullName.last RegexMatch", "[A-Z]{1}ank\\w+$", //
                            "fullName.first", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user1.toString());
        }

    }

    @Test
    public void field_a_equals_field_b_should_work_with_filter() throws Exception {

        // test condition that use field a and field b
        var id1 = "field_a_equals_field_b_should_work_with_filter1";
        var id2 = "field_a_equals_field_b_should_work_with_filter2";
        var partition = "FieldTest";
        try {

            var data1 = Map.of("id", id1, "mail", "mail1", "uniqueKey", "aaa");
            var data2 = Map.of("id", id2, "mail", "mail2", "uniqueKey", "mail2");

            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);
            
            {
                // equal
                var cond = Condition.filter("mail", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(host, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(1);
                assertThat(results.get(0)).containsEntry("id", id2);
            }
            {
                // not equal
                var cond = Condition.filter("mail !=", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(host, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(1);
                assertThat(results.get(0)).containsEntry("id", id1);
            }
            {
                // greater than
                var cond = Condition.filter("mail >=", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(host, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(2);
            }
        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }

    }

    @Test
    void count_should_ignore_skip_and_limit() throws Exception {

        // test skip
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "id IN", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(10) //
                    .offset(1);
            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test limit
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "id IN", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(1) //
                    .offset(0);
            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test skip + limit
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "id IN", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(1) //
                    .offset(2);
            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

    }


    @Test
    void aggregate_should_work() throws Exception {
        // test aggregate(simple group by)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("fullName.last");

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expect = Map.of("Hanks", 2, "Henry", 1);

            var lastName1 = result.get(0).getOrDefault("last", "").toString();
            // the result of count should be integer
            assertThat(result.get(0).get("facetCount")).isInstanceOf(Integer.class).isEqualTo(expect.get(lastName1));

            var lastName2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(result.get(1).get("facetCount")).isEqualTo(expect.get(lastName2));

        }

        // test aggregate(max)
        {
            var aggregate = Aggregate.function("MAX(c.age) AS maxAge, COUNT(1) AS facetCount").groupBy("fullName.last");

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expectAge = Map.of("Hanks", 30, "Henry", 45);
            var expectCount = Map.of("Hanks", 2, "Henry", 1);

            var lastName1 = result.get(0).get("last");
            assertThat(result.get(0).get("maxAge")).isEqualTo(expectAge.get(lastName1));
            assertThat(result.get(0).get("facetCount")).isEqualTo(expectCount.get(lastName1));

            var lastName2 = result.get(1).get("last");
            assertThat(result.get(1).get("maxAge")).isEqualTo(expectAge.get(lastName2));
            assertThat(result.get(1).get("facetCount")).isEqualTo(expectCount.get(lastName2));

        }

        // test aggregate(sum)
        {
            var aggregate = Aggregate.function("SUM(c.age) AS ageSum").groupBy("fullName.last");

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expect = Map.of("Hanks", 42, "Henry", 45);

            var lastName1 = result.get(0).getOrDefault("last", "").toString();
            // the result of count should be integer
            assertThat(result.get(0).get("ageSum")).isInstanceOf(Integer.class).isEqualTo(expect.get(lastName1));

            var lastName2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(result.get(1).get("ageSum")).isEqualTo(expect.get(lastName2));

        }
    }

    @Test
    void aggregate_should_work_without_group_by() throws Exception {

        // test count(without group by)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "Andersen"), "Families").toMap();

            assertThat(result).hasSize(1);

            var value = result.get(0).getOrDefault("facetCount", "").toString();

            assertThat(Integer.parseInt(value)).isEqualTo(1);

        }

        // test count(without group by, no hit documents)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "NotExist_LastName"), "Families").toMap();

            assertThat(result).hasSize(1);

            var value = result.get(0).getOrDefault("facetCount", "").toString();
            assertThat(Integer.parseInt(value)).isEqualTo(0);

        }

        // test max(without group by)
        {
            var aggregate = Aggregate.function("MAX(c.creationDate) AS maxDate");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "Andersen"), "Families").toMap();

            assertThat(result).hasSize(1);

            var value = result.get(0).getOrDefault("maxDate", "").toString();
            assertThat(Integer.parseInt(value)).isGreaterThan(0);

        }

        // test max(without group by, no hit documents)
        {
            var aggregate = Aggregate.function("MAX(c.creationDate) AS maxDate");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "NotExist"), "Families").toMap();

            assertThat(result).hasSize(1);

            var value = result.get(0).getOrDefault("maxDate", "");
            assertThat(value).isInstanceOfSatisfying(Map.class, (v) -> assertThat(v).isEmpty());

        }
    }

    @Test
    public void aggregate_should_work_with_condition_afterwards() throws Exception {

        // test aggregate with afterwards filter
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount")
                    .groupBy("fullName.last")
                    .conditionAfterAggregate(Condition.filter("facetCount >", 1) // only find the group by result that facetCount > 1
                            .sort("last", "DESC")
                            // Note that only field like "status" "name" can be sort after group by.
                            // aggregation value like "count" cannot be used in sort after group by.
                            .offset(0).limit(2)); // sort

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(1);

            // Hanks family has 2 members
            var expect = Map.of("Hanks", 2);

            var lastName1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(result.get(0).get("last")).isEqualTo("Hanks");
            // the result of count should be integer
            assertThat(result.get(0).get("facetCount")).isInstanceOf(Integer.class).isEqualTo(expect.get(lastName1));
        }
        // test aggregate(with order by)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("fullName.last")
                    .conditionAfterAggregate(Condition.filter().sort("last", "ASC"));

            // test find
            var result = db.aggregate(host, aggregate, Condition.filter(), "Users").toMap();
            assertThat(result).hasSize(2);

            var last1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(last1).isEqualTo("Hanks");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(2);

            var last2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(last2).isEqualTo("Henry");
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);

        }

        // test aggregate with multiple group by
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount")
                    .groupBy("fullName.last", "age")
                    .conditionAfterAggregate(Condition.filter().sort("age", "DESC"));

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(3);

            /*
            The first element should be:
            {
              "facetCount" : 1,
              "last" : "Henry",
              "age" : 45
             }
             */

            var lastName1 = result.get(0).getOrDefault("last", "").toString();
            assertThat(lastName1).isEqualTo("Henry");
            // the result of count should be integer
            assertThat(result.get(0).get("age")).isInstanceOf(Integer.class).isEqualTo(45);
        }
    }

    @Test
    void aggregate_should_work_with_key_brackets() throws Exception {

        // test MAX(c['creationDate'])
        {
            var aggregate = Aggregate.function("MAX(c['creationDate']) AS maxDate");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "Andersen"), "Families").toMap();

            assertThat(result).hasSize(1);

            var value = result.get(0).getOrDefault("maxDate", "").toString();
            assertThat(Integer.parseInt(value)).isGreaterThan(0);

        }
    }

    @Test
    void aggregate_should_work_with_lower_cases() throws Exception {

        // test count(1) as facetCount
        {
            var aggregate = Aggregate.function("count(1) as facetCount")
                    .groupBy("fullName.last", "age")
                    .conditionAfterAggregate(Condition.filter().sort("age", "DESC"));

            // test find
            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(3);

        }
    }

    @Test
    void aggregate_should_work_with_nested_functions() throws Exception {

        // test ARRAY_LENGTH(c.children)
        {
            var aggregate = Aggregate.function("SUM(ARRAY_LENGTH(c['children'])) AS facetCount");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter(), "Families").toMap();

            assertThat(result).hasSize(1);
            var value = result.get(0).getOrDefault("facetCount", "").toString();
            assertThat(Integer.parseInt(value)).isEqualTo(4);

        }
    }

    @Test
    void find_should_work_when_reading_double_type() throws Exception {

        var id = "find_should_work_when_reading_double_type";
        var partition = "FindTests";
        // double field in db should be found and still be double type
        try {
            var data = Map.of("id", id, "score", 10.0);

            // test find
            db.upsert(host, data, partition);
            var result = db.find(host, Condition.filter("id", id), partition).toMap();
            assertThat(result).hasSize(1);

            // the result of score be double
            // MongoDB works correctly
            assertThat(result.get(0).get("score")).isInstanceOf(Double.class).isEqualTo(10d);

        } finally {
            db.delete(host, id, partition);
        }
    }

    @Test
    public void find_should_work_with_join() throws Exception {

        // query with join
        {
            var cond = new Condition();

            cond = Condition.filter("area.city.street.rooms.no", "001", "room*no-01.area", 10) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("area.city.street.rooms", "room*no-01"))
                    .returnAllSubArray(false);

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            var rooms = JsonUtil.toListOfMap(JsonUtil.toJson(JsonUtil.toMap(JsonUtil.toMap(JsonUtil.toMap(result.get(0).get("area")).get("city")).get("street")).get("rooms")));
            assertThat(rooms).hasSize(1);
            assertThat(rooms.get(0)).containsEntry("no", "001");

            cond = Condition.filter("area.city.street.rooms.no", "001") //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("area.city.street.rooms"))
                    .returnAllSubArray(true);
            ;

            result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            rooms = JsonUtil.toListOfMap(JsonUtil.toJson(JsonUtil.toMap(JsonUtil.toMap(JsonUtil.toMap(result.get(0).get("area")).get("city")).get("street")).get("rooms")));
            assertThat(rooms).hasSize(2);
            assertThat(rooms.get(0)).containsEntry("no", "001");
            assertThat(rooms.get(1)).containsEntry("no", "002");

            cond = Condition.filter("parents.firstName", "Thomas", "parents.firstName", "Mary Kay", "children.gender", "female", "children.grade <", 6, "room*no-01.area", 10) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("parents", "children", "room*no-01"))
                    .returnAllSubArray(false);

            result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(result.get(0)).containsEntry("_partition", "Families");
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).stream().anyMatch(item -> item.get("firstName").toString().equals("Mary Kay"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("gender").toString().equals("female"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("grade").toString().equals("5"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("room*no-01"))).stream().anyMatch(item -> item.get("area").toString().equals("10"))).isTrue();
        }

        //Or query with join
        {
            var cond = Condition.filter(SubConditionType.OR, List.of( //
                            Condition.filter("parents.firstName", "Thomas"), //
                            Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false);

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(2);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("firstName", "Thomas");
            assertThat(result.get(0).get("id")).hasToString("AndersenFamily");
            assertThat(result.get(1).get("creationDate")).hasToString("1431620462");
        }

        //AND query with join
        {
            var cond = Condition.filter(SubConditionType.AND, List.of( //
                            Condition.filter("parents.familyName", "Wakefield"), //
                            Condition.filter("isRegistered", false))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents"))
                    .returnAllSubArray(false);

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(result.get(0).get("id")).hasToString("WakefieldFamily");
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("familyName", "Wakefield");
        }

        // NOT query with join
        {
            var cond = Condition
                    .filter("$NOT", Map.of("address.state", "WA"), "$AND", Map.of("parents.familyName !=", "Wakefield"))
                    .sort("id", "ASC")
                    .join(Set.of("parents"))
                    .returnAllSubArray(false);

            var items = db.find(host, cond, "Families").toMap();

            assertThat(items).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("parents"))).get(0)).containsEntry("familyName", "Miller");
            assertThat(items.get(0).get("id")).hasToString("WakefieldFamily");
        }

        var user = new User("joinTestArrayContainId", "firstNameJoin", "lostNameJoin");
        var userMap = JsonUtil.toMap(user);
        userMap.put("rooms", List.of(Map.of("no", List.of(1, 2, 3)), Map.of("no", List.of(1, 2, 4))));
        db.upsert(host, userMap, "Users");

        // ARRAY_CONTAINS query with join
        {
            var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", 3) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("rooms"))
                    .returnAllSubArray(false);

            // test find
            var items = db.find(host, cond, "Users").toMap();

            assertThat(items).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asList().contains(3);
            assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId");

            db.delete(host, "joinTestArrayContainId", "Users");
        }

    }

    @Test
    public void find_should_work_with_join_using_array_contains() throws Exception {

        // ARRAY_CONTAINS query with join

        var id1 = "joinTestArrayContainId";
        var id2 = "joinTestArrayContainId2";
        var partition = "Users";

        try {
            var user = new User(id1, "firstNameJoin", "lastNameJoin");
            var userMap = JsonUtil.toMap(user);
            userMap.put("rooms", List.of(Map.of("no", List.of(1, 2, 3)), Map.of("no", List.of(1, 2, 4))));
            db.upsert(host, userMap, partition);


            var user2 = new User(id2, "firstNameJoin2", "lastNameJoin2");
            var userMap2 = JsonUtil.toMap(user2);
            userMap2.put("rooms", List.of(Map.of("no", List.of(4, 5, 6)), Map.of("no", List.of(6, 7, 8))));
            db.upsert(host, userMap2, partition);

            {
                // simple ARRAY_CONTAINS
                var cond = Condition.filter("rooms.no ARRAY_CONTAINS", 3) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("rooms"))
                        .returnAllSubArray(false);

                // test find
                var items = db.find(host, cond, "Users").toMap();

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId");

            }

            {
                // ARRAY_CONTAINS_ANY
                var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", List.of(3, 9)) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("rooms"))
                        .returnAllSubArray(false);

                // test find
                var items = db.find(host, cond, "Users").toMap();

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId");

            }

            {
                // ARRAY_CONTAINS_ALL
                var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ALL", List.of(2, 3)) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("rooms"))
                        .returnAllSubArray(false);

                // test find
                var items = db.find(host, cond, "Users").toMap();

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId");

            }

        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }
    }

    @Test
    public void find_should_work_with_join_using_limit_and_fields() throws Exception {

        // find with join, small limit
        {
            var cond = Condition.filter(SubConditionType.OR, List.of( //
                            Condition.filter("parents.firstName", "Thomas"), //
                            Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false)
                    .offset(0)
                    .limit(1);

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("firstName", "Thomas");
            assertThat(result.get(0).get("id")).hasToString("AndersenFamily");

        }

        // find with join, set offset
        {
            var cond = Condition.filter(SubConditionType.OR, List.of( //
                            Condition.filter("parents.firstName", "Thomas"), //
                            Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false)
                    .offset(1)
                    .limit(10);

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            // TODO, result is different from cosmosdb
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(0);
            //assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("givenName", "Robin");
            assertThat(result.get(0).get("id")).hasToString("WakefieldFamily");
        }

        // find with join, fields
        {
            var cond = Condition.filter(SubConditionType.OR, List.of( //
                            Condition.filter("parents.firstName", "Thomas"), //
                            Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false)
                    .offset(1)
                    .limit(1)
                    .fields("id", "parents", "address");

            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(0);
            //assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("givenName", "Robin");
            assertThat(result.get(0).get("id")).hasToString("WakefieldFamily");
            assertThat(result.get(0).get("address")).isNotNull();

            // fields excluded
            assertThat(result.get(0).get("children")).isNull();
            assertThat(result.get(0).get("lastName")).isNull();

        }

    }

    @Disabled
    public void find_should_work_with_join_together_with_aggregate() throws Exception {
        // aggregate query with join
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("lastName");
            var cond = new Condition();
            cond = Condition.filter("children.gender", "female", "children.grade <", 6) //
                    .sort("lastName", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("parents", "children"));
            // test find
            var result = db.aggregate(host, aggregate, cond, "Families").toMap();
            assertThat(result).hasSize(2);
            assertThat(result.get(0).getOrDefault("lastName", "")).isEqualTo("");
            assertThat(result.get(1).getOrDefault("lastName", "")).isEqualTo("Andersen");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
        }
    }

    @Disabled
    /**
     * TODO, This case is not implemented at present
     */
    void find_should_work_with_join_using_elem_match() throws Exception {
        // condition on the same sub array should be both applied to the element(e.g. children.gender = "female" AND children.grade = 5)
        // If we want to implement this, we can introduce a new SubConditionType like "$ElemMatch" in mongodb
        {
            var cond = new Condition();
            cond = Condition.filter("children.gender", "female", "children.grade =", 5) //
                    .sort("lastName", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false)
            ;
            // test find
            var result = db.find(host, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getOrDefault("id", "")).isEqualTo("WakefieldFamily");
        }
    }

    @Disabled
    /**
     * TODO rawSql is not implemented for mongodb
     */
    void raw_query_spec_should_work() throws Exception {
        // test json from cosmosdb official site
        // https://docs.microsoft.com/ja-jp/azure/cosmos-db/sql-query-getting-started

        var partition = "Families";

        var queryText = "SELECT c.gender, c.grade\n" + "    FROM Families f\n"
                + "    JOIN c IN f.children WHERE f.address.state = @state ORDER BY f.id ASC";


        var params = new SqlParameterCollection(new SqlParameter("@state", "NY"));

        var cond = Condition.rawSql(queryText, params);

        var children = db.find(host, cond, partition).toMap();

        assertThat(children).hasSize(2);

        assertThat(children.get(0).get("gender")).hasToString("female");
        assertThat(children.get(1).get("grade")).hasToString("8");

    }

    @Test
    void dynamic_field_and_is_defined_should_work() throws Exception {
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
            db.upsert(host, data, partition);
            db.upsert(host, data2, partition);
            db.upsert(host, data3, partition);
            // add a nullValue to document for further test
            var operations = PatchOperations.create().set("/nullField", null);
            db.patch(host, id3, operations, partition);

            {
                // dynamic fields
                var cond = Condition.filter("id", id, String.format("%s.name", formId), "Tom");
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Tom").containsEntry("sex", "Male").containsEntry("address", "NY");
            }

            {
                // IS_DEFINED = true
                var cond = Condition.filter("id", id, String.format("%s IS_DEFINED", formId), true);
                var items = db.find(host, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_DEFINED = false
                var cond = Condition.filter("id", id, "test IS_DEFINED", false);
                var items = db.find(host, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_NUMBER = true
                var cond = Condition.filter("id", id, "age IS_NUMBER", true);
                var items = db.find(host, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }
            {
                // IS_NUMBER = false
                var cond = Condition.filter("id", id, String.format("%s IS_NUMBER", formId), false);
                var items = db.find(host, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // IS_NULL = true
                {   // not exist field
                    // IS_NULL means "field exists" AND "value is null"
                    var cond = Condition.filter("id", id, "notExistField IS_NULL", true);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(0);
                }
                {   // null field
                    var cond = Condition.filter("nullField IS_NULL", true);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(1);
                    assertThat(items.get(0)).containsEntry("id", id3);
                }

                {  // not null field
                    var cond = Condition.filter("id", id, "age IS_NULL", true);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(0);
                }
            }

            {   // IS_NULL = false
                {
                    // notExist field
                    var cond = Condition.filter("id", id, "notExist IS_NULL", false);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(1);
                    assertThat(items.get(0).get("id")).isEqualTo(id);
                }
                {
                    // null field
                    var cond = Condition.filter("id", id3, "nullField IS_NULL", false);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(0);
                }

                {  // not null field
                    var cond = Condition.filter("id", id, "age IS_NULL", false);
                    var items = db.find(host, cond, partition).toMap();
                    assertThat(items).hasSize(1);
                    assertThat(items.get(0).get("id")).isEqualTo(id);
                }

            }

            {
                // IS_DEFINED = false in OR condition. result: 2 item
                var cond = Condition.filter("id LIKE", "D00%", SubConditionType.OR, List.of(
                        Condition.filter(String.format("%s IS_DEFINED", formId), false),
                        Condition.filter(String.format("%s.empty", formId), true)
                )).sort("id", "ASC");
                var items = db.find(host, cond, partition).toMap();
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
                var items = db.find(host, cond, partition).toMap();
                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).isEqualTo(id);
            }

            {
                // nested fields
                var cond = Condition.filter("id", id, String.format("%s.name", formId), "Tom").fields("id", String.format("%s.name", formId), String.format("%s.sex", formId), "sheet-2.skills");
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Tom").containsEntry("sex", "Male").doesNotContainEntry("address", "NY");

                var map2 = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get("sheet-2")));
                assertThat(map2).containsKey("skills");
                assertThat(map2.values().toString()).contains("Java", "Python");

            }


        } finally {
            db.delete(host, id, partition);
            db.delete(host, id2, partition);
            db.delete(host, id3, partition);
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
            db.upsert(host, data, partition);

            {
                // dynamic fields with ARRAY_CONTAINS_ALL
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), Set.of("Java", "Python"));
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }
            {
                // dynamic fields with ARRAY_CONTAINS_ALL, not hit
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), Set.of("Java", "CSharp"));
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }
            {
                // dynamic fields with ARRAY_CONTAINS_ANY
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ANY", formId), Set.of("Java", "CSharp"));
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }

            {
                // dynamic fields with ARRAY_CONTAINS
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS", formId), "Java");
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(1);
                var map = JsonUtil.toMap(JsonUtil.toJson(items.get(0).get(formId)));
                assertThat(map).containsEntry("name", "Jerry");
            }

            {
                // empty with ARRAY_CONTAINS
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS", formId), "");
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }

            {
                // empty list with ARRAY_CONTAINS_ANY
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ANY", formId), List.of());
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }

            {
                // empty list with ARRAY_CONTAINS_ALL
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), List.of());
                var items = db.find(host, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }

        } finally {
            db.delete(host, id, partition);
        }

    }

    @Test
    void number_should_be_read_write_correctly() throws Exception {
        var partition = "NumberTest";
        var prefix = "number_should_be_read_write_correctly";

        var id1 = prefix + "integer";
        var id2 = prefix + "double";
        var id3 = "";

        var data1 = Map.of("id", id1, "contents", Map.of("age", 20));
        var data2 = Map.of("id", id2, "contents", Map.of("age", 40.0));

        try {
            {
                // integer should be upserted as an integer and read as an integer
                var upserted1 = db.upsert(host, data1, partition).toMap();
                assertThat((Map<String, Object>) upserted1.get("contents")).containsEntry("age", 20);
                var read1 = db.read(host, id1, partition).toMap();
                assertThat((Map<String, Object>) read1.get("contents")).containsEntry("age", 20);
            }

            try (var is = CosmosDatabaseTest.class.getResourceAsStream("sheet-integer.json")) {
                // integer should be read as an integer in nested json

                var sheet = JsonUtil.fromJson(is, new TypeReference<LinkedHashMap<String, Object>>() {
                });
                var upserted1 = db.upsert(host, sheet, partition).toMap();
                id3 = upserted1.getOrDefault("id", "").toString();
                var decimal = (Map<String, Object>) ((Map<String, Object>) upserted1.get("contents")).get("Decimal002");
                assertThat(decimal).containsEntry("value", 3);


            }

            {
                // double should be upserted as a double and read as a double
                var upserted2 = db.upsert(host, data2, partition).toMap();

                // the result is double for MongoDB. which works correctly.
                // CosmosDB does not work correctly. This would be an issue of CosmosDB or azure-cosmos
                assertThat((Map<String, Object>) upserted2.get("contents")).containsEntry("age", 40d);
                var read2 = db.read(host, id2, partition).toMap();
                assertThat((Map<String, Object>) read2.get("contents")).containsEntry("age", 40d);
            }


        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
            db.delete(host, id3, partition);
        }


    }

    @Test
    void patch_should_work() throws Exception {
        var partition = "PatchTests";
        var id = "patch_should_work";

        try {
            var data1 = Map.of("id", id, "name", "John", "contents", Map.of("age", 20),
                    "score", 85.5, "skills", List.of("Java", "Kotlin", "TypeScript"));
            db.upsert(host, data1, partition).toMap();
            {
                // Add should work

                var operations = PatchOperations.create()
                        .add("/skills/1", "Golang") // insert Golang at index 1
                        .add("/contents/sex", "Male"); // add a new field
                var item = db.patch(host, id, operations, partition).toMap();
                assertThat((List<String>) item.get("skills")).hasSize(4);
                assertThat(((List<String>) item.get("skills")).get(1)).isEqualTo("Golang");
                assertThat((Map<String, Object>) item.get("contents")).containsEntry("sex", "Male");

            }

            {
                // Set should work

                var operations = PatchOperations.create()
                        .set("/contents", Map.of("age", 19)) // reset contents to a new map
                        .set("/skills/2", "Rust"); // replace index 2 from Kotlin to Rust
                var item = db.patch(host, id, operations, partition).toMap();
                assertThat((List<String>) item.get("skills")).hasSize(4);
                assertThat(((List<String>) item.get("skills")).get(2)).isEqualTo("Rust");
                assertThat((Map<String, Object>) item.get("contents")).hasSize(1).containsEntry("age", 19);

            }

            {
                // Replace should work
                {
                    // replace an existing field
                    var operations = PatchOperations.create()
                            .replace("/contents", Map.of("age", 20)); // replace contents to a new map
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat((Map<String, Object>) item.get("contents")).hasSize(1).containsEntry("age", 20);
                }
                {
                    // replacing a not existing field is allowed for mongodb
                    var operations = PatchOperations.create()
                            .replace("/notExistField", Map.of("age", 21)); // try to replace a not existing field
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat((Map<String, Object>) item.get("notExistField")).hasSize(1).containsEntry("age", 21);
                }

            }

            {
                // Remove should work
                {
                    // remove an existing field
                    var operations = PatchOperations.create()
                            .remove("/score") // remove an existing field
                            //.remove("/skills/2") // remove an array item at index 2 (Not supported by mongodb)
                            ;
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat(item.get("score")).isNull();

                }
                {
                    // removing a not existing field is allowed in mongodb
                    var operations = PatchOperations.create()
                            .remove("/notExistField"); // try to replace a not existing field
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat(item.get("notExistField")).isNull();

                }

                {
                    // increment should work
                    var operations = PatchOperations.create()
                            .increment("/contents/age", 2) // increment a number field
                            ;
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat((Map<String, Object>) item.get("contents")).containsEntry("age", 22L);
                }

                {
                    // operations exceeding 10 ops should fail
                    var operations = PatchOperations.create()
                            .add("/testField1", 1) //
                            .set("/testField2", 2) //
                            .replace("/name", "3") //
                            .set("/testField", 4) //
                            .set("/testField", 5) //
                            .set("/testField", 6) //
                            .set("/testField", 7) //
                            .set("/testField", 8) //
                            .set("/testField", 9) //
                            .set("/testField", 10) //
                            .set("/testField", 11) //
                            ;
                    assertThatThrownBy(() ->
                            db.patch(host, id, operations, partition).toMap())
                            .isInstanceOfSatisfying(IllegalArgumentException.class, (e) -> {
                                assertThat(e.getMessage()).contains("exceed", "10", "11");
                            });
                }
            }


        } finally {
            db.delete(host, id, partition);
        }

    }

    @Test
    void patch_should_work_with_pojo() throws Exception {
        var partition = "PatchPojoTests";
        var id = "patch_should_work_with_pojo";

        try {
            var data1 = Map.of("id", id, "name", "John",
                    "contents", List.of(new CheckBox("id1", "name1", CheckBox.Align.VERTICAL)),
                    "score", 85.5);
            db.upsert(host, data1, partition).toMap();

            {
                // Set should work

                var operations = PatchOperations.create()
                        .set("/contents", List.of(
                                new CheckBox("id1", "name1", CheckBox.Align.HORIZONTAL),
                                new CheckBox("id2", "name2", CheckBox.Align.VERTICAL)
                        )); // reset contents to a new list
                var item = db.patch(host, id, operations, partition).toMap();

                assertThat((List<Map<String, Object>>) item.get("contents")).hasSize(2);

                
                var checkboxList = JsonUtil.fromJson2List(JsonUtil.toJson(item.get("contents")), CheckBox.class);

                assertThat(checkboxList).hasSize(2);

                assertThat(checkboxList.get(0).id).isEqualTo("id1");
                assertThat(checkboxList.get(0).align).isEqualTo(CheckBox.Align.HORIZONTAL);
                assertThat(checkboxList.get(1).id).isEqualTo("id2");
                assertThat(checkboxList.get(1).align).isEqualTo(CheckBox.Align.VERTICAL);


            }
        } finally {
            db.delete(host, id, partition);
        }
    }

    @Test
    void increment_should_work() throws Exception {
        var partition = "IncrementTests";
        var id = "increment_should_work";


        try {
            var data1 = Map.of("id", id, "name", "John", "contents", Map.of("age", 20), "score", 85.5, "number", 3_147_483_647L);
            db.upsert(host, data1, partition).toMap();
            {
                // increment by 1, integer field
                var inc1 = db.increment(host, id, "/contents/age", 1, partition).toMap();
                assertThat((Map<String, Object>) inc1.get("contents")).containsEntry("age", 21L);

                // increment by -3, integer field
                var inc2 = db.increment(host, id, "/contents/age", -3, partition).toMap();
                assertThat((Map<String, Object>) inc2.get("contents")).containsEntry("age", 18L);

                // increment by 1, long field
                var inc3 = db.increment(host, id, "/number", 1, partition).toMap();
                assertThat(inc3).containsEntry("number", 3_147_483_648L);

                // increment by 5, double field
                var inc4 = db.increment(host, id, "/score", 5, partition).toMap();
                assertThat(inc4).containsEntry("score", 90.5);

            }

            {
                // failed when incrementing a string field
                assertThatThrownBy(() -> {
                    db.increment(host, id, "/name", 5, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("non-numeric type");
                });
            }

            {
                // 400 will be thrown when path is not correct
                assertThatThrownBy(() -> {
                    db.increment(host, id, "score", 5, partition);
                }).isInstanceOfSatisfying(IllegalArgumentException.class, (e) -> {
                    assertThat(e.getMessage()).contains("Json path(score) must start with /");
                });
            }

            {
                // when incrementing a not existing item
                assertThatThrownBy(() -> {
                    db.increment(host, "not exist", "/number", 1, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(404);
                    assertThat(e.getMessage()).contains("Not Found");
                });
            }

        } finally {
            db.delete(host, id, partition);
        }
    }

    @Test
    void batchCreate_should_work() throws Exception {
        int size = 100;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("batchCreate_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            var result = db.batchCreate(host, userList, "Users");
            assertThat(result).hasSize(size);
        } finally {
            db.batchDelete(host, userList, "Users");
        }
    }

    @Test
    void batchDelete_should_work() throws Exception {
        int size = 100;
        var userList = new ArrayList<User>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("batchDelete_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }
        var idList = userList.stream().map(u -> u.id).collect(Collectors.toList());

        try {
            db.batchCreate(host, userList, "Users");
            // idList can be used to do batchDelete
            var deleteResult = db.batchDelete(host, idList, "Users");
            assertThat(deleteResult).hasSize(size);
        } finally {
            db.batchDelete(host, userList, "Users");
        }
    }

    @Test
    void batchUpsert_should_work() throws Exception {
        int size = 100;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("batchUpsert_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            var createResultList = db.batchCreate(host, userList, "Users");

            var upsertList = new ArrayList<User>(size);
            for (CosmosDocument cosmosDocument : createResultList) {
                var user = cosmosDocument.toObject(User.class);
                user.firstName = user.firstName.replace("testFirstName", "modifiedFirstName");
                upsertList.add(user);
            }

            var upsertResultList = db.batchUpsert(host, upsertList, "Users");
            for (CosmosDocument cosmosDocument : upsertResultList) {
                var user = cosmosDocument.toObject(User.class);
                assertThat(user.firstName).contains("modifiedFirstName");
            }

        } finally {
            db.batchDelete(host, userList, "Users");
        }
    }

    @Test
    void bulkCreate_should_work() throws Exception {
        int size = 120;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("bulkCreate_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            var result = db.bulkCreate(host, userList, "Users");
            assertThat(result.fatalList).hasSize(0);
            assertThat(result.retryList).hasSize(0);
            assertThat(result.successList).hasSize(size);
        } finally {
            db.bulkDelete(host, userList, "Users");
        }
    }

    @Test
    void bulkDelete_should_work() throws Exception {
        int size = 120;
        var userList = new ArrayList<User>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("bulkDelete_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        var idList = userList.stream().map(u -> u.id).collect(Collectors.toList());

        try {
            db.bulkCreate(host, userList, "Users");
            // idList can be used to do bulkDelete
            var deleteResult = db.bulkDelete(host, idList, "Users");
            assertThat(deleteResult.fatalList).hasSize(0);
            assertThat(deleteResult.retryList).hasSize(0);
            assertThat(deleteResult.successList).hasSize(size);
        } finally {
            db.bulkDelete(host, userList, "Users");
        }
    }

    @Test
    void bulkUpsert_should_work() throws Exception {
        int size = 120;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("bulkUpsert_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            var createResult = db.bulkCreate(host, userList, "Users");
            assertThat(createResult.fatalList).hasSize(0);
            assertThat(createResult.retryList).hasSize(0);
            assertThat(createResult.successList).hasSize(size);

            var upsertList = new ArrayList<User>(size);
            for (CosmosDocument cosmosDocument : createResult.successList) {
                var user = cosmosDocument.toObject(User.class);
                user.firstName = user.firstName.replace("testFirstName", "modifiedFirstName");
                upsertList.add(user);
            }

            var upsertResult = db.bulkUpsert(host, upsertList, "Users");
            assertThat(upsertResult.fatalList).hasSize(0);
            assertThat(upsertResult.retryList).hasSize(0);
            assertThat(upsertResult.successList).hasSize(size);

            for (CosmosDocument cosmosDocument : upsertResult.successList) {
                var user = cosmosDocument.toObject(User.class);
                assertThat(user.firstName).contains("modifiedFirstName");
            }

        } finally {
            db.bulkDelete(host, userList, "Users");
        }
    }

    @Test
    void bulkUpsert_should_work_containing_both_create_and_update() throws Exception {

        var partition = "Users";
        int size = 3;
        var userList = new ArrayList<User>(size);

        for (int i = 0; i < size; i++) {
            userList.add(new User("bulkUpsert_should_work_both" + i, "testFirstName" + i, "testLastName" + i));
        }
        try {

            // create userList[0] beforehand, so this is an upsert
            // let userList[1] and userList[2] untouched, so these are creations
            db.upsert(host, userList.get(0), partition);

            // prepare for upsert
            for (int i = 0; i < size; i++) {
                userList.get(i).firstName = "modifiedName" + i;
            }

            var result = db.bulkUpsert(host, userList, partition);
            assertThat(result.successList).hasSize(3);

        } finally {
            db.batchDelete(host, userList, partition);
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
