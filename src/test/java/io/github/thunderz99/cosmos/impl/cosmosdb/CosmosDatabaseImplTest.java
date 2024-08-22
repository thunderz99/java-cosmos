package io.github.thunderz99.cosmos.impl.cosmosdb;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.*;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CosmosDatabaseImplTest {

    static Cosmos cosmos;
    static CosmosDatabase db;

    static String dbName = "CosmosDB";
    static String coll = "UnitTest";

    static FullNameUser user1 = null;
    static FullNameUser user2 = null;
    static FullNameUser user3 = null;
    static FullNameUser user4 = null;

    Logger log = LoggerFactory.getLogger(CosmosDatabaseImplTest.class);

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
        cosmos = new CosmosBuilder().withConnectionString(EnvUtil.get("COSMOSDB_CONNECTION_STRING")).build();
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
    void getId_should_work() {
        String testId = "getId_should_work_id";
        var user = new User(testId, "firstName", "lastName");
        var id = CosmosDatabaseImpl.getId(user);
        assertThat(id).isEqualTo(testId);

        id = CosmosDatabaseImpl.getId(testId);
        assertThat(id).isEqualTo(testId);
    }

    @Test
    void doCheckBeforeBatch_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            CosmosDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
        }

        {
            // number of operations exceed the limit should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 101; i++) {
                testData.add(new User());
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("The number of data operations should not exceed 100.");
        }

        {
            // invalid id should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\n", "", ""));
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }
    }

    @Test
    void doCheckBeforeBulk_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            CosmosDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulk(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulk(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
        }

        {
            // invalid id should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\n", "", ""));
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }
    }

    @Test
    void checkValidId_should_work() {
        // normal check
        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("valid_id", "", ""));
            }

            CosmosDatabaseImpl.checkValidId(testData);
        }

        {
            List<String> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add("valid_id");
            }

            CosmosDatabaseImpl.checkValidId(testData);
        }

        // boundary checks
        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\t", "", ""));
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.checkValidId(testData))
                    .hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\n", "", ""));
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\r", "", ""));
            }

            assertThatThrownBy(() -> CosmosDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
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
    public void regex_should_work_with_filter() throws Exception {

        // test regex match
        {
            var cond = Condition.filter("fullName.last RegexMatch", "[A-Z]{1}ank\\w+$", //
                            "fullName.first", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(coll, cond, "Users").toList(FullNameUser.class);

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

            db.upsert(coll, data1, partition);
            db.upsert(coll, data2, partition);

            {
                // equal
                var cond = Condition.filter("mail", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(coll, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(1);
                assertThat(results.get(0)).containsEntry("id", id2);
            }
            {
                // not equal
                var cond = Condition.filter("mail !=", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(coll, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(1);
                assertThat(results.get(0)).containsEntry("id", id1);
            }
            {
                // greater than
                var cond = Condition.filter("mail >=", Condition.key("uniqueKey") //
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var results = db.find(coll, cond, partition).toMap();

                assertThat(results.size()).isEqualTo(2);
            }
        } finally {
            db.delete(coll, id1, partition);
            db.delete(coll, id2, partition);
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
            var count = db.count(coll, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test limit
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "id IN", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(1) //
                    .offset(0);
            // count
            var count = db.count(coll, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test skip + limit
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "id IN", List.of(user1.id, user2.id, user3.id)).sort("id", "DESC") //
                    .limit(1) //
                    .offset(2);
            // count
            var count = db.count(coll, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

    }

    @Test
    void aggregate_should_work() throws Exception {
        // test aggregate(simple)
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
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
            var result = db.aggregate(coll, aggregate, "Users").toMap();
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

        // test aggregate(without AS)
        {
            var aggregate = Aggregate.function("MAX(c.age) AS maxAge, COUNT(1)").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expectAge = Map.of("Hanks", 30, "Henry", 45);
            var expectCount = Map.of("Hanks", 2, "Henry", 1);

            var lastName1 = result.get(0).get("last");
            assertThat(result.get(0).get("maxAge")).isEqualTo(expectAge.get(lastName1));
            assertThat(result.get(0).get("$1")).isEqualTo(expectCount.get(lastName1));

            var lastName2 = result.get(1).get("last");
            assertThat(result.get(1).get("maxAge")).isEqualTo(expectAge.get(lastName2));
            assertThat(result.get(1).get("$1")).isEqualTo(expectCount.get(lastName2));

        }

        // test aggregate(without AS for all field)
        {
            var aggregate = Aggregate.function("MAX(c.age), COUNT(1)").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expectAge = Map.of("Hanks", 30, "Henry", 45);
            var expectCount = Map.of("Hanks", 2, "Henry", 1);

            var lastName1 = result.get(0).get("last");
            assertThat(result.get(0).get("$1")).isEqualTo(expectAge.get(lastName1));
            assertThat(result.get(0).get("$2")).isEqualTo(expectCount.get(lastName1));

            var lastName2 = result.get(1).get("last");
            assertThat(result.get(1).get("$1")).isEqualTo(expectAge.get(lastName2));
            assertThat(result.get(1).get("$2")).isEqualTo(expectCount.get(lastName2));

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

        // test aggregate(sum)
        {
            var aggregate = Aggregate.function("SUM(c.age) AS ageSum").groupBy("fullName.last");

            // test find
            var result = db.aggregate(coll, aggregate, "Users").toMap();
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
    void find_should_work_when_reading_double_type() throws Exception {

        var id = "find_should_work_when_reading_double_type";
        var partition = "FindTests";
        // double field in db should be found and still be double type
        try {
            var data = Map.of("id", id, "score", 10.0);

            // test find
            db.upsert(coll, data, partition);
            var result = db.find(coll, Condition.filter("id", id), partition).toMap();
            assertThat(result).hasSize(1);

            // the result of score be double
            // TODO: the result of score is integer at present, this would be an issue of CosmosDB or azure-cosmos
            assertThat(result.get(0).get("score")).isInstanceOf(Integer.class).isEqualTo(10);

        } finally {
            db.delete(coll, id, partition);
        }
    }


    @Test
    void convertAggregateResultsToInteger_should_work() {
        // Setup

        // Test data setup
        List<LinkedHashMap<String, Object>> testMaps = new ArrayList<>();
        LinkedHashMap<String, Object> map1 = new LinkedHashMap<>();
        map1.put("itemsCount", 1L);
        map1.put("name", "TestName1");
        LinkedHashMap<String, Object> map2 = new LinkedHashMap<>();
        map2.put("itemsCount", Long.MAX_VALUE);
        map2.put("name", "TestName2");
        LinkedHashMap<String, Object> map3 = new LinkedHashMap<>();
        map3.put("itemsCount", 100L);
        map3.put("itemsWithinRange", Integer.MAX_VALUE);
        testMaps.add(map1);
        testMaps.add(map2);
        testMaps.add(map3);

        // Call the method under test
        List<? extends LinkedHashMap> resultMaps = CosmosDatabaseImpl.convertAggregateResultsToInteger(testMaps);

        // Assertions
        assertThat(resultMaps).isNotNull();
        assertThat(resultMaps.size()).isEqualTo(3);

        assertThat(resultMaps.get(0).get("itemsCount")).isInstanceOf(Integer.class).isEqualTo(1);
        assertThat(resultMaps.get(1).get("itemsCount")).isInstanceOf(Long.class).isEqualTo(Long.MAX_VALUE); // Should remain Long because it's out of Integer range
        assertThat(resultMaps.get(2).get("itemsCount")).isInstanceOf(Integer.class).isEqualTo(100);
        assertThat(resultMaps.get(2).get("itemsWithinRange")).isInstanceOf(Integer.class).isEqualTo(Integer.MAX_VALUE); // Should remain Integer
    }

    /**
     * remove keys like "_ts", "_rid" in a map, in order to do a clean comparision.
     *
     * @param map
     * @return
     */
    Map<String, Object> removeSystemKeys(Map<String, Object> map) {
        var keys = map.keySet().stream().collect(Collectors.toList());

        for (var key : keys) {
            if (StringUtils.startsWith(key, "_")) {
                map.remove(key);
            }
        }
        return map;
    }

    @Test
    public void find_and_to_map_should_retain_order_of_key() throws Exception {

        var id = "find_and_to_map_should_retain_order_of_key";
        var partition = "FindTests";
        try {
            var doc = new LinkedHashMap<String, Object>();
            doc.put("id", id);
            doc.put("b", 1);
            doc.put("c", 2);
            doc.put("d", 3);
            doc.put("a", 4);

            var upserted = removeSystemKeys(db.upsert(coll, doc, partition).toMap());

            assertThat(upserted.keySet().stream().collect(Collectors.toList())).containsExactly("id", "b", "c", "d", "a");

            var readRaw = removeSystemKeys(db.read(coll, id, partition).toMap());

            var findRaw = removeSystemKeys(db.find(coll, Condition.filter("id", id), partition).toMap().get(0));

            assertThat(JsonUtil.toJson(readRaw)).isEqualTo(JsonUtil.toJson(findRaw));


        } finally {
            db.delete(coll, id, partition);
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

            var result = db.find(coll, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            var rooms = JsonUtil.toListOfMap(JsonUtil.toJson(JsonUtil.toMap(JsonUtil.toMap(JsonUtil.toMap(result.get(0).get("area")).get("city")).get("street")).get("rooms")));
            assertThat(rooms).hasSize(1);
            assertThat(rooms.get(0)).containsEntry("no", "001");

            cond = Condition.filter("area.city.street.rooms.no", "001") //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("area.city.street.rooms"));

            result = db.find(coll, cond, "Families").toMap();
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

            result = db.find(coll, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(result.get(0)).containsEntry("_partition", "Families");
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).stream().anyMatch(item -> item.get("firstName").toString().equals("Mary Kay"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("gender").toString().equals("female"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("grade").toString().equals("5"))).isTrue();
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("room*no-01"))).stream().anyMatch(item -> item.get("area").toString().equals("10"))).isTrue();
        }
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
            var result = db.aggregate(coll, aggregate, cond, "Families").toMap();
            assertThat(result).hasSize(2);
            assertThat(result.get(0).getOrDefault("lastName", "")).isEqualTo("");
            assertThat(result.get(1).getOrDefault("lastName", "")).isEqualTo("Andersen");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
        }

        //Or query with join
        {
            var cond = Condition.filter(SubConditionType.OR, List.of( //
                            Condition.filter("parents.firstName", "Thomas"), //
                            Condition.filter("id", "WakefieldFamily"))) //
                    .sort("id", "ASC")//
                    .join(Set.of("parents", "children"))
                    .returnAllSubArray(false);

            var result = db.find(coll, cond, "Families").toMap();
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

            var result = db.find(coll, cond, "Families").toMap();
            assertThat(result).hasSize(1);
            assertThat(result.get(0).get("id")).hasToString("WakefieldFamily");
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("familyName", "Wakefield");
        }

        // NOT query with join
        {
            var cond = Condition
                    .filter("$NOT", Map.of("address.state", "WA"), "$NOT 2", Map.of("parents.familyName", "Wakefield"))
                    .sort("id", "ASC")
                    .join(Set.of("parents"))
                    .returnAllSubArray(false);

            var items = db.find(coll, cond, "Families").toMap();

            assertThat(items).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("parents")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("parents"))).get(0)).containsEntry("familyName", "Miller");
            assertThat(items.get(0).get("id")).hasToString("WakefieldFamily");
        }

        var user = new User("joinTestArrayContainId", "firstNameJoin", "lostNameJoin");
        var userMap = JsonUtil.toMap(user);
        userMap.put("rooms", List.of(Map.of("no", List.of(1, 2, 3)), Map.of("no", List.of(1, 2, 4))));
        db.upsert(coll, userMap, "Users").toObject(User.class);

        // ARRAY_CONTAINS query with join
        {
            var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", 3) //
                    .sort("id", "ASC") //
                    .limit(10) //
                    .offset(0)
                    .join(Set.of("rooms"))
                    .returnAllSubArray(false);

            // test find
            var items = db.find(coll, cond, "Users").toMap();

            assertThat(items).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asList().contains(3);
            assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId");

            db.delete(coll, "joinTestArrayContainId", "Users");
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

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-OR.json"));
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

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-AND.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-AND2.json"));
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

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-NOT.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(coll, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("WakefieldFamily");
        }

    }

    @Test
    void check_invalid_id_should_work() throws Exception {
        var ids = List.of("\ttabbefore", "tabafter\t", "tab\nbetween", "\ncrbefore", "crafter\r", "cr\n\rbetween", "/test");
        for (var id : ids) {
            assertThatThrownBy(() -> CosmosDatabaseImpl.checkValidId(id)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("id cannot contain");
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
                    db.delete(coll, map.getOrDefault("id", "").toString(), partition);
                }
            }
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

                var partialMapC = Map.of("city", "Tokyo", CosmosImpl.ETAG, "invalid etag");

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

                Map<String, Object> partialMapA = Maps.newHashMap(Map.of("age", 30, CosmosImpl.ETAG, etag));

                Map<String, Object> partialMapB = Map.of("sort", "199", "employeeCode", "X0456", CosmosImpl.ETAG, etag);

                var patchedB = db.updatePartial(coll, id, partialMapB, partition, PartialUpdateOption.checkETag(true)).toMap();

                // B should update sort and add employeeCode only
                assertThat(patchedB).containsEntry("sort", "199").containsEntry("employeeCode", "X0456")
                        .containsEntry("age", 25) // age keep not change
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                ;

                // new etag should be generated
                assertThat(patchedB.getOrDefault(CosmosImpl.ETAG, "").toString()).isNotEmpty().isNotEqualTo(etag);


                assertThatThrownBy(() -> db.updatePartial(coll, id, partialMapA, partition, PartialUpdateOption.checkETag(true)).toMap())
                        .isInstanceOfSatisfying(CosmosException.class, (e) -> {
                            assertThat(e.getStatusCode()).isEqualTo(412);
                        });

                // empty etag will be ignored
                partialMapA.put(CosmosImpl.ETAG, "");
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

            {
                // empty with ARRAY_CONTAINS
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS", formId), "");
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }

            {
                // empty list with ARRAY_CONTAINS_ANY
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ANY", formId), List.of());
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(0);
            }

            {
                // empty list with ARRAY_CONTAINS_ALL
                var cond = Condition.filter("id", id, String.format("%s.value ARRAY_CONTAINS_ALL", formId), List.of());
                var items = db.find(coll, cond, partition).toMap();

                assertThat(items).hasSize(0);
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
        var id3 = "";

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

            try (var is = CosmosDatabaseTest.class.getResourceAsStream("sheet-integer.json")) {
                // integer should be read as an integer in nested json

                var sheet = JsonUtil.fromJson(is, new TypeReference<LinkedHashMap<String, Object>>() {
                });
                var upserted1 = db.upsert(coll, sheet, partition).toMap();
                id3 = upserted1.getOrDefault("id", "").toString();
                var decimal = (Map<String, Object>) ((Map<String, Object>) upserted1.get("contents")).get("Decimal002");
                assertThat(decimal).containsEntry("value", 3);


            }

            {
                // double should be upserted as a double and read as a double
                var upserted2 = db.upsert(coll, data2, partition).toMap();

                // At present, v4 sdk should read this value as the same as the origin(40.0)
                // TODO: the result is integer at present, this would be an issue of CosmosDB or azure-cosmos
                assertThat((Map<String, Object>) upserted2.get("contents")).containsEntry("age", 40);
                var read2 = db.read(coll, id2, partition).toMap();
                assertThat((Map<String, Object>) read2.get("contents")).containsEntry("age", 40);
            }


        } finally {
            db.delete(coll, id1, partition);
            db.delete(coll, id2, partition);
            db.delete(coll, id3, partition);
        }


    }

    @Test
    void increment_should_work() throws Exception {
        var partition = "IncrementTests";
        var id = "increment_should_work";


        try {
            var data1 = Map.of("id", id, "name", "John", "contents", Map.of("age", 20), "score", 85.5, "number", 3_147_483_647L);
            db.upsert(coll, data1, partition).toMap();
            {
                // increment by 1, integer field
                var inc1 = db.increment(coll, id, "/contents/age", 1, partition).toMap();
                assertThat((Map<String, Object>) inc1.get("contents")).containsEntry("age", 21);

                // increment by -3, integer field
                var inc2 = db.increment(coll, id, "/contents/age", -3, partition).toMap();
                assertThat((Map<String, Object>) inc2.get("contents")).containsEntry("age", 18);

                // increment by 1, long field
                var inc3 = db.increment(coll, id, "/number", 1, partition).toMap();
                assertThat(inc3).containsEntry("number", 3_147_483_648L);

                // increment by 5, double field
                var inc4 = db.increment(coll, id, "/score", 5, partition).toMap();
                assertThat(inc4).containsEntry("score", 90.5);

            }

            {
                // failed when incrementing a string field
                assertThatThrownBy(() -> {
                    db.increment(coll, id, "/name", 5, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("is not a number");
                });
            }

            {
                // 400 will be thrown when path is not correct
                assertThatThrownBy(() -> {
                    db.increment(coll, id, "score", 5, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(400);
                    assertThat(e.getMessage()).contains("inputs is invalid");
                });
            }

            {
                // 404 will be thrown when incrementing a not existing item
                assertThatThrownBy(() -> {
                    db.increment(coll, "not exist", "/number", 1, partition);
                }).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                    assertThat(e.getStatusCode()).isEqualTo(404);
                    assertThat(e.getMessage()).contains("Not Found");
                });
            }

        } finally {
            db.delete(coll, id, partition);
        }

    }

    @Test
    void patch_should_work() throws Exception {
        var partition = "PatchTests";
        var id = "patch_should_work";


        try {
            var data1 = Map.of("id", id, "name", "John", "contents", Map.of("age", 20),
                    "score", 85.5, "skills", List.of("Java", "Kotlin", "TypeScript"));
            db.upsert(coll, data1, partition).toMap();
            {
                // Add should work

                var operations = PatchOperations.create()
                        .add("/skills/1", "Golang") // insert Golang at index 1
                        .add("/contents/sex", "Male"); // add a new field
                var item = db.patch(coll, id, operations, partition).toMap();
                assertThat((List<String>) item.get("skills")).hasSize(4);
                assertThat(((List<String>) item.get("skills")).get(1)).isEqualTo("Golang");
                assertThat((Map<String, Object>) item.get("contents")).containsEntry("sex", "Male");

            }

            {
                // Set should work

                var operations = PatchOperations.create()
                        .set("/contents", Map.of("age", 19)) // reset contents to a new map
                        .set("/skills/2", "Rust"); // replace index 2 from Kotlin to Rust
                var item = db.patch(coll, id, operations, partition).toMap();
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
                    var item = db.patch(coll, id, operations, partition).toMap();
                    assertThat((Map<String, Object>) item.get("contents")).hasSize(1).containsEntry("age", 20);
                }
                {
                    // replace a not existing field should fail
                    var operations = PatchOperations.create()
                            .replace("/notExistField", Map.of("age", 20)); // try to replace a not existing field
                    assertThatThrownBy(() ->
                            db.patch(coll, id, operations, partition).toMap()).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                        assertThat(e.getStatusCode()).isEqualTo(400);
                        assertThat(e.getMessage()).contains("notExistField", "absent");
                    });

                }

            }

            {
                // Remove should work
                {
                    // remove an existing field
                    var operations = PatchOperations.create()
                            .remove("/score") // remove an existing field
                            .remove("/skills/2") // remove an array item at index 2
                            ;
                    var item = db.patch(coll, id, operations, partition).toMap();
                    assertThat(item.get("score")).isNull();
                    assertThat(((List<String>) item.get("skills"))).hasSize(3);
                    assertThat(((List<String>) item.get("skills")).get(2)).isEqualTo("TypeScript");

                }
                {
                    // remove a not existing field should fail
                    var operations = PatchOperations.create()
                            .remove("/notExistField"); // try to replace a not existing field
                    assertThatThrownBy(() ->
                            db.patch(coll, id, operations, partition).toMap()).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                        assertThat(e.getStatusCode()).isEqualTo(400);
                        assertThat(e.getMessage()).contains("notExistField", "absent");
                    });

                }

                {
                    // increment should work
                    var operations = PatchOperations.create()
                            .increment("/contents/age", 2) // increment a number field
                            ;
                    var item = db.patch(coll, id, operations, partition).toMap();
                    assertThat((Map<String, Object>) item.get("contents")).containsEntry("age", 22);
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
                            db.patch(coll, id, operations, partition).toMap())
                            .isInstanceOfSatisfying(IllegalArgumentException.class, (e) -> {
                                assertThat(e.getMessage()).contains("exceed", "10", "11");
                            });
                }
            }


        } finally {
            db.delete(coll, id, partition);
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
            var result = db.batchCreate(coll, userList, "Users");
            assertThat(result).hasSize(size);
        } finally {
            db.batchDelete(coll, userList, "Users");
        }
    }

    @Test
    void batchDelete_should_work() throws Exception {
        int size = 100;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("batchDelete_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            db.batchCreate(coll, userList, "Users");
            var deleteResult = db.batchDelete(coll, userList, "Users");
            assertThat(deleteResult).hasSize(size);
        } finally {
            try {
                db.batchDelete(coll, userList, "Users");
            } catch (Exception ignored) {
            }
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
            var createResultList = db.batchCreate(coll, userList, "Users");

            var upsertList = new ArrayList<User>(size);
            for (CosmosDocument cosmosDocument : createResultList) {
                var user = cosmosDocument.toObject(User.class);
                user.firstName = user.firstName.replace("testFirstName", "modifiedFirstName");
                upsertList.add(user);
            }

            var upsertResultList = db.batchUpsert(coll, upsertList, "Users");
            for (CosmosDocument cosmosDocument : upsertResultList) {
                var user = cosmosDocument.toObject(User.class);
                assertThat(user.firstName).contains("modifiedFirstName");
            }

        } finally {
            db.batchDelete(coll, userList, "Users");
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
            var result = db.bulkCreate(coll, userList, "Users");
            assertThat(result.fatalList).hasSize(0);
            assertThat(result.retryList).hasSize(0);
            assertThat(result.successList).hasSize(size);
        } finally {
            db.bulkDelete(coll, userList, "Users");
        }
    }

    @Test
    void bulkDelete_should_work() throws Exception {
        int size = 120;
        var userList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            userList.add(new User("bulkDelete_should_work_" + i, "testFirstName" + i, "testLastName" + i));
        }

        try {
            db.bulkCreate(coll, userList, "Users");
            var deleteResult = db.bulkDelete(coll, userList, "Users");
            assertThat(deleteResult.fatalList).hasSize(0);
            assertThat(deleteResult.retryList).hasSize(0);
            assertThat(deleteResult.successList).hasSize(size);
        } finally {
            try {
                db.bulkDelete(coll, userList, "Users");
            } catch (Exception ignored) {
            }
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
            var createResult = db.bulkCreate(coll, userList, "Users");
            assertThat(createResult.fatalList).hasSize(0);
            assertThat(createResult.retryList).hasSize(0);
            assertThat(createResult.successList).hasSize(size);

            var upsertList = new ArrayList<User>(size);
            for (CosmosDocument cosmosDocument : createResult.successList) {
                var user = cosmosDocument.toObject(User.class);
                user.firstName = user.firstName.replace("testFirstName", "modifiedFirstName");
                upsertList.add(user);
            }

            var upsertResult = db.bulkUpsert(coll, upsertList, "Users");
            assertThat(upsertResult.fatalList).hasSize(0);
            assertThat(upsertResult.retryList).hasSize(0);
            assertThat(upsertResult.successList).hasSize(size);

            for (CosmosDocument cosmosDocument : upsertResult.successList) {
                var user = cosmosDocument.toObject(User.class);
                assertThat(user.firstName).contains("modifiedFirstName");
            }

        } finally {
            db.bulkDelete(coll, userList, "Users");
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

        var merged = CosmosDatabaseImpl.merge(map1, map2);

        assertThat(merged).containsEntry("name", "Jane") // updated
                .containsEntry("sort", "010") // reserved
        ;
        assertThat((Map<String, Object>) merged.get("contents"))
                .containsEntry("phone", "12345") // reserved
                .containsEntry("skill", "backend") //updated
                .containsEntry("addresses", Lists.newArrayList("NY", "Houston")) //updated
        ;

    }

    @Test
    void sql_limit_should_not_be_exceeded() throws Exception {
        var formId = "d674dad9-c7de-49bc-b5c2-edc42c67ca82";
        var options = List.of("", "proper", "arubaito", "part time", "contract", "intern", "outsoucing");
        var orgs = List.of("b0800989-716c-4cd8-9c0b-7b79e1788821");
        var cond = Condition.filter(String.format("sheetContents.%s.SingleSelect009.value", formId), options, "assignedOrgIds ARRAY_CONTAINS_ANY", orgs);

        var partition = "SheetContents";

        var count = db.count(coll, cond, partition);
        assertThat(count).isEqualTo(0);

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
