package io.github.thunderz99.cosmos.impl.postgres;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.*;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.CheckBox;
import io.github.thunderz99.cosmos.dto.EvalSkip;
import io.github.thunderz99.cosmos.dto.FullNameUser;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosImpl;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.util.TTLUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.thunderz99.cosmos.condition.SubConditionType.AND;
import static io.github.thunderz99.cosmos.condition.SubConditionType.OR;
import static io.github.thunderz99.cosmos.impl.postgres.util.TableUtilTest.SINGLE_TASK_EXECUTOR;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

public class PostgresDatabaseImplTest {

    public static Cosmos cosmos;
    public static PostgresDatabaseImpl db;

    public static String dbName = "java_cosmos";

    /**
     * Temporary schema name for unit test, which will be created and deleted on the fly
     */
    static String host = "unittest_postgres_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

    static FullNameUser user1 = null;
    static FullNameUser user2 = null;
    static FullNameUser user3 = null;
    static FullNameUser user4 = null;

    Logger log = LoggerFactory.getLogger(PostgresImplTest.class);

    public static class User {
        public String id;
        public String firstName;
        public String lastName;

        public String createdAt;

        public User() {
        }

        public User(String id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public User(String id, String firstName, String lastName, String createdAt) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.createdAt = createdAt;
        }
    }

    static String longPartitionName = "Users_" + RandomStringUtils.randomAlphanumeric(64).toLowerCase() + "s";

    @BeforeAll
    public static void beforeAll() throws Exception {
        cosmos = new CosmosBuilder().withDatabaseType("postgres")
                .withExpireAtEnabled(true)
                .withEtagEnabled(true)
                .withConnectionString(EnvUtil.getOrDefault("POSTGRES_CONNECTION_STRING", PostgresImplTest.LOCAL_CONNECTION_STRING))
                .withCollate("C")
                .build();

        // we do not need to create a collection here, so the second param is empty
        // we create collections in specific test cases
        db = (PostgresDatabaseImpl) cosmos.createIfNotExist(dbName, host);

        db.createTableIfNotExists(host, "Users");
        db.createTableIfNotExists(host, "Users2");
        db.createTableIfNotExists(host, "Families");
        db.createTableIfNotExists(host, "SheetContents");
        db.createTableIfNotExists(host, "PatchTests");
        db.createTableIfNotExists(host, "PatchPojoTests");
        db.createTableIfNotExists(host, "IncrementTests");
        db.createTableIfNotExists(host, "NumberTest");
        db.createTableIfNotExists(host, "TTLTests");
        db.createTableIfNotExists(host, "TTLDeleteTests");
        db.createTableIfNotExists(host, "EtagTests");
        db.createTableIfNotExists(host, "FieldTest");
        db.createTableIfNotExists(host, "FindTests");
        db.createTableIfNotExists(host, "SheetContents2");
        db.createTableIfNotExists(host, "UserSorts");
        db.createTableIfNotExists(host, "SearchViews");
        db.createTableIfNotExists(host, longPartitionName);

        initFamiliesData();
        initData4ComplexQuery();

    }

    @AfterAll
    public static void afterAll() throws Exception {
        if(cosmos != null) {
            cosmos.deleteCollection(dbName, host);
            cosmos.closeClient();
        }
    }

    @Test
    void ping_should_work() throws Exception {
        assertThat(db.ping(host)).isTrue();

        assertThatThrownBy(() -> db.ping("NotExistCollection$"))
                .isInstanceOf(CosmosException.class)
                .hasMessageContaining("schema").hasMessageContaining("not found");
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

            // check _ts exist for timestamp is correct
            var map = db.read(host, user.id, "Users").toMap();
            assertThat(map.get("_ts")).isInstanceOfSatisfying(Integer.class, i -> assertThat(i.longValue()).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0)));

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
        var id = PostgresDatabaseImpl.getId(user);
        assertThat(id).isEqualTo(testId);

        id = PostgresDatabaseImpl.getId(testId);
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
            PostgresDatabaseImpl.checkValidId(testData);
        }

        {
            List<String> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add("valid_id");
            }

            PostgresDatabaseImpl.checkValidId(testData);
        }

        // boundary checks
        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\t", "", ""));
            }

            assertThatThrownBy(() -> PostgresDatabaseImpl.checkValidId(testData))
                    .hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\n", "", ""));
            }

            assertThatThrownBy(() -> PostgresDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }

        {
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                testData.add(new User("invalid_id\r", "", ""));
            }

            assertThatThrownBy(() -> PostgresDatabaseImpl.checkValidId(testData)).hasMessageContaining("id cannot contain \\t or \\n or \\r");
        }
    }

    @Test
    void create_should_throw_when_data_is_null() throws Exception {
        User user = null;
        assertThatThrownBy(() -> db.create(host, user, "Users")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("create data " + host);

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
            assertThat(map.get("_ts")).isInstanceOfSatisfying(Integer.class, i ->
                    assertThat(i.longValue()).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0)));
            var timestamp1 = (Integer) map.get("_ts");


            var upsert1 = new User(user.id, "firstUpsert", "lastUpsert");

            // full upsert
            Thread.sleep(1000);
            var upserted1 = db.upsert(host, upsert1, "Users").toObject(User.class);
            assertThat(upserted1.id).isEqualTo(upsert1.id);
            assertThat(upserted1.firstName).isEqualTo(upsert1.firstName);
            assertThat(upserted1.lastName).isEqualTo(upsert1.lastName);

            // _ts should also be updated
            var upserted1Map = db.read(host, upsert1.id, "Users").toMap();
            assertThat(upserted1Map.get("_ts")).isInstanceOfSatisfying(Integer.class, i ->
                    assertThat(i.longValue()).isGreaterThan(timestamp1.longValue()).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0)));


        } finally {
            db.delete(host, user.id, "Users");
        }

    }

    @Test
    void upsert_should_work_4_long_partition_name() throws Exception {

        var partition = longPartitionName;
        var user = new User("unittest_upsert_01", "first01", "last01");
        db.delete(host, user.id, partition);

        try {
            var upserted = db.upsert(host, user, partition).toObject(User.class);
            assertThat(upserted.id).isEqualTo(user.id);
            assertThat(upserted.firstName).isEqualTo(user.firstName);

            // _ts exist for timestamp
            var map = db.read(host, user.id, partition).toMap();
            assertThat(map.get("_ts")).isInstanceOfSatisfying(Integer.class, i ->
                    assertThat(i.longValue()).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0)));

            var upsert1 = new User(user.id, "firstUpsert", "lastUpsert");

            // full upsert
            var upserted1 = db.upsert(host, upsert1, partition).toObject(User.class);
            assertThat(upserted1.id).isEqualTo(upsert1.id);
            assertThat(upserted1.firstName).isEqualTo(upsert1.firstName);
            assertThat(upserted1.lastName).isEqualTo(upsert1.lastName);

            // find
            var docs = db.find(host, Condition.filter("lastName", "lastUpsert"), partition).toList(User.class);
            assertThat(docs).hasSize(1);
            assertThat(docs.get(0).firstName).isEqualTo(upsert1.firstName);


        } finally {
            db.delete(host, user.id, partition);
        }

    }

    @Test
    void get_database_name_should_work() throws Exception {
        assertThat(db.getDatabaseName()).isEqualTo(dbName);
    }

    @Test
    void doCheckBeforeBatch_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            PostgresDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBatch_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBatch(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBatch(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
        }

        {
            // number of operations exceed the limit should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = new ArrayList<>();
            for (int i = 0; i < 101; i++) {
                testData.add(new User());
            }

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBatch(testColl, testData, testPartition)).hasMessageContaining("The number of data operations should not exceed 100.");
        }
    }

    @Test
    void doCheckBeforeBulk_should_work() {
        // normal check
        {
            String testColl = "testColl";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            PostgresDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition);
        }

        // boundary checks
        {
            // blank coll should raise exception
            String testColl = "";
            String testPartition = "testPartition";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("coll should be non-blank");
        }

        {
            // blank partition should raise exception
            String testColl = "testColl";
            String testPartition = "";
            List<User> testData = List.of(new User("doCheckBeforeBulk_should_work_id", "first01", "last01"));

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulk(testColl, testData, testPartition)).hasMessageContaining("partition should be non-blank");
        }

        {
            // empty data should raise exception
            String testColl = "testColl";
            String testPartition = "testPartition";

            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulk(testColl, List.of(), testPartition)).hasMessageContaining("should not be empty collection");
            assertThatThrownBy(() -> PostgresDatabaseImpl.doCheckBeforeBulk(testColl, null, testPartition)).hasMessageContaining("should not be empty collection");
        }

    }

    @Test
    void updatePartial_should_work() throws Exception {
        var partition = "SheetContents";

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
    void updatePartial_should_work_with_optimistic_concurrency_control() throws Exception {
        var partition = "SheetContents";

        var id = "updatePartial_should_work_with_optimistic_concurrency_control"; // form with content
        var age = 20;
        var formId = "03a69e73-18b8-44e5-a0b1-1b2739ca6e60"; //uuid
        var formContent = Map.of("name", "Tom", "sex", "Male", "address", "NY", "tags",
                List.of(Map.of("id", "t001", "name", "backend"), Map.of("id", "t002", "name", "frontend")));
        var data = Map.of("id", id, "age", age, "sort", "010", formId, formContent, "sheet-2", Map.of("skills", Set.of("Java", "Python")));

        try {
            var upserted = db.upsert(host, data, partition).toMap();

            assertThat(upserted).containsKeys("id", "age", formId, "_etag");

            {
                // read by A,B, update by B and A with different field should succeed
                // PartialUpdateOption.checkETag is false(default)

                var partialMapA = Map.of("age", 25);

                var partialMapB = Map.of("sort", "099", "employeeCode", "X0123");

                var patchedB = db.updatePartial(host, id, partialMapB, partition).toMap();

                var patchedA = db.updatePartial(host, id, partialMapA, partition).toMap();


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
                var patchedC = db.updatePartial(host, id, partialMapC, partition).toMap();

                assertThat(patchedC).containsEntry("city", "Tokyo").containsEntry("age", 25).containsEntry("sort", "099");

            }

            {
                // read by A,B, update by B and A with different field should succeed
                // PartialUpdateOption.checkETag is true

                var originData = db.read(host, id, partition).toMap();

                var etag = originData.getOrDefault("_etag", "").toString();
                assertThat(etag).isNotEmpty();

                Map<String, Object> partialMapA = Maps.newHashMap(Map.of("age", 30, CosmosImpl.ETAG, etag));

                Map<String, Object> partialMapB = Map.of("sort", "199", "employeeCode", "X0456", CosmosImpl.ETAG, etag);

                var patchedB = db.updatePartial(host, id, partialMapB, partition, PartialUpdateOption.checkETag(true)).toMap();

                // B should update sort and add employeeCode only
                assertThat(patchedB).containsEntry("sort", "199").containsEntry("employeeCode", "X0456")
                        .containsEntry("age", 25) // age keep not change
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition)
                ;

                // new etag should be generated
                assertThat(patchedB.getOrDefault(CosmosImpl.ETAG, "").toString()).isNotEmpty().isNotEqualTo(etag);


                assertThatThrownBy(() -> db.updatePartial(host, id, partialMapA, partition, PartialUpdateOption.checkETag(true)).toMap())
                        .isInstanceOfSatisfying(CosmosException.class, (e) -> {
                            assertThat(e.getStatusCode()).isEqualTo(412);
                        });

                // if 412 exception, the original document should not be updated
                var originMap = db.read(host, id, partition).toMap();
                // value of patchedB
                assertThat(originMap).containsEntry("age", 25);


                // empty etag will be ignored
                partialMapA.put(CosmosImpl.ETAG, "");
                var patchedA = db.updatePartial(host, id, partialMapA, partition, PartialUpdateOption.checkETag(true)).toMap();

                // the result should be correct partial updated
                assertThat(patchedA).containsEntry("age", 30) // age updated
                        .containsEntry("sort", "199").containsEntry("employeeCode", "X0456") // sort and employeeCode remains B's result
                        .containsKey("_ts").containsKey(formId).containsKey("sheet-2")
                        .containsEntry("_partition", partition);
            }

        } finally {
            db.delete(host, id, partition);
        }

    }

    @Test
    void find_should_work_with_filter() throws Exception {

        // test basic find
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "fullName.first", "Elise", //
                            "_ts >", 0
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user1.toString());
        }

        // test sort with lower and upper cases
        {
            var cond = Condition.filter("mail ENDSWITH", "@example.com"
                    ).sort("mail", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(3);
            assertThat(users.get(0).id).isEqualTo(user2.id);
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

        // test IN find for numeric

        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "age", List.of(12.0, 13, 14)).sort("id", "DESC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user1.toString());

            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(1);
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

            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(1);
        }

        // test compare operator for double
        {
            var cond = Condition.filter( //
                            "fullName.last !=", "Henry", //
                            "age >=", 29.5).sort("_ts", "DESC") //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user2.toString());
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

            // count
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
            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(2);
        }

        // test enum
        {
            var cond = Condition.filter( //
                            "skills ARRAY_CONTAINS", FullNameUser.Skill.Python, //
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
    void find_should_work_with_filter_comparing_array() throws Exception {
        // test != array
        {
            var cond = Condition.filter("skills !=", List.of()
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(3);
            assertThat(users.get(0)).hasToString(user1.toString());
        }

        // test = array
        {
            var cond = Condition.filter("skills =", List.of("Java", "Go", "Python")
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var users = db.find(host, cond, "Users").toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user3.toString());
        }
    }

    @Test
    void find_should_work_with_filter_comparing_null() throws Exception {

        var partition = "SearchViews";
        var id1 = "comparing_null1" + RandomStringUtils.randomAlphanumeric(6);
        var id2 = "comparing_null2" + RandomStringUtils.randomAlphanumeric(6);

        try {

            var data1 = Maps.newLinkedHashMap(Map.of("id", id1, "product", "TALENT"));
            data1.put("accountId", null);
            var data2 = Map.of("id", id2, "accountId", "hana", "product", "");

            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);

            // test != null
            {
                var cond = Condition.filter("accountId !=", null
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var users = db.find(host, cond, partition).toMap();

                assertThat(users.size()).isEqualTo(1);
                assertThat(users.get(0).get("id")).isEqualTo(data2.get("id"));
            }

            // test = null
            {
                var cond = Condition.filter("accountId =", null
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var users = db.find(host, cond, partition).toMap();

                assertThat(users.size()).isEqualTo(1);
                assertThat(users.get(0).get("id")).isEqualTo(data1.get("id"));
            }

            // using IS_DEFINED = false
            {
                var cond = Condition.filter("accountId IS_DEFINED", false
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var users = db.find(host, cond, partition).toMap();

                assertThat(users.size()).isEqualTo(1);
                assertThat(users.get(0).get("id")).isEqualTo(data1.get("id"));
            }


        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }
    }


    @Test
    void find_should_work_with_filter_comparing_text_to_int() throws Exception {

        var partition = "Users";
        var id1 = "comparing_text_to_int1_" + RandomStringUtils.randomAlphanumeric(6);
        var id2 = "comparing_text_to_int2_" + RandomStringUtils.randomAlphanumeric(6);

        try {

            var data1 = Map.of("id", id1, "age", 20);
            var data2 = Map.of("id", id2, "age", "");

            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);

            // test comparing age
            {
                var cond = Condition.filter("id STARTSWITH", "comparing_text_to_int",
                        "age >", 1
                        ).sort("id", "ASC") //
                        .limit(10) //
                        .offset(0);

                var users = db.find(host, cond, partition).toMap();

                assertThat(users.size()).isEqualTo(1);
                assertThat(users.get(0).get("id")).isEqualTo(data1.get("id"));
            }


        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }
    }


    @Test
    void find_should_work_with_sort_lower_upper() throws Exception {

        var partition = "SheetContents";
        var id1 = "sort_lower_upper1" + RandomStringUtils.randomAlphanumeric(6);
        var id2 = "sort_lower_upper2" + RandomStringUtils.randomAlphanumeric(6);
        var id3 = "sort_lower_upper3" + RandomStringUtils.randomAlphanumeric(6);

        try {

            var data1 = Map.of("id", id1, "type", "Phone", "value", "Galaxy");
            var data2 = Map.of("id", id2, "type", "Phone", "value", "Pixel");
            var data3 = Map.of("id", id3, "type", "Phone", "value", "iPhone");

            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);
            db.upsert(host, data3, partition);

            // test sort ASC
            {
                var cond = Condition.filter("type", "Phone"
                        ).sort("value", "ASC") //
                        .limit(10) //
                        .offset(0);

                var docs = db.find(host, cond, partition).toMap();

                // expected: "Galaxy" < "Pixel" < "iPhone"
                // wrong case: "Galaxy" < "iPhone" < "Pixel" (which ignores upper and lower case)
                assertThat(docs.size()).isEqualTo(3);
                assertThat(docs.get(0).get("value")).isEqualTo("Galaxy");
                assertThat(docs.get(1).get("value")).isEqualTo("Pixel");
                assertThat(docs.get(2).get("value")).isEqualTo("iPhone");
            }

            // test sort DESC
            {
                var cond = Condition.filter("type", "Phone"
                        ).sort("value", "DESC") //
                        .limit(10) //
                        .offset(0);

                var docs = db.find(host, cond, partition).toMap();

                // expected: "Galaxy" < "Pixel" < "iPhone"
                // wrong case: "Galaxy" < "iPhone" < "Pixel" (which ignores upper and lower case)
                assertThat(docs.size()).isEqualTo(3);
                assertThat(docs.get(0).get("value")).isEqualTo("iPhone");
                assertThat(docs.get(1).get("value")).isEqualTo("Pixel");
                assertThat(docs.get(2).get("value")).isEqualTo("Galaxy");
            }

        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
            db.delete(host, id3, partition);
        }
    }


    @Test
    void find_should_work_with_sort_stably_with_the_same_ts() throws Exception {

        var count = 42;
        var partition = "SheetContents";
        var idPrefix = "stably_with_the_same_ts_";

        var items = new ArrayList<Map<String, String>>();


        try {

            for(int i = 0; i < count; i++) {
                var id = (idPrefix+"%03d").formatted(i);
                var item = Map.of("id", id, "type", "Phone", "value", "iPhone");
                items.add(item);
            }

            db.bulkCreate(host, items, partition);

            // test sort ASC
            {
                var cond = Condition.filter("id STARTSWITH", idPrefix
                        ).sort("_ts", "ASC") //
                        .limit(10) //
                        .offset(0);

                var docs = db.find(host, cond, partition).toMap();


                var cond2 = Condition.filter("id STARTSWITH", idPrefix
                        ).sort("_ts", "ASC") //
                        .limit(10) //
                        .offset(10);

                var docs2 = db.find(host, cond2, partition).toMap();

                docs.addAll(docs2);

                docs.sort(Comparator.comparing(o -> o.get("id").toString()));

                assertThat(docs.size()).isEqualTo(20);

                for(int i = 0; i < 20; i++) {
                    assertThat(docs.get(i).get("id")).isEqualTo( (idPrefix+"%03d").formatted(i) );
                }
            }


        } finally {
           db.bulkDelete(host, items, partition);
        }
    }


    @Test
    void find_should_work_for_array_contains_any_all() throws Exception {
        var partition = "Families";

        // test ARRAY_CONTAINS_ANY
        {
            var cond = Condition.filter( //
                            "skills ARRAY_CONTAINS_ANY", List.of("Typescript", "Blanco" ), //
                            "age <", 100) //
                    .sort("id", "ASC" ) //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users" ).toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user2.id);

            var count = db.count(host, cond, "Users" );
            assertThat(count).isEqualTo(2);
        }
        // test ARRAY_CONTAINS_ALL
        {
            var cond = Condition.filter( //
                            "skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java" ), //
                            "age <", 100) //
                    .sort("id", "ASC" ) //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users" ).toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0).id).isEqualTo(user2.id);

            var count = db.count(host, cond, "Users" );
            assertThat(count).isEqualTo(1);
        }

        // test ARRAY_CONTAINS_ALL negative
        {
            var cond = Condition.filter( //
                            "skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java" ), //
                            "age <", 100) //
                    .sort("id", "ASC" ) //
                    .not() //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users" ).toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users" );
            assertThat(count).isEqualTo(2);
        }

        // test ARRAY_CONTAINS_ALL negative using $NOT
        {
            var cond = Condition.filter( //
                            "$NOT", Map.of("$AND", //
                                    List.of( //
                                            Map.of("skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java" )), //
                                            Map.of("age <", 100)))) //
                    .sort("id", "ASC" ) //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users" ).toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users" );
            assertThat(count).isEqualTo(2);
        }

        // test ARRAY_CONTAINS_ALL negative using $NOT, simple version
        {
            var cond = Condition.filter( //
                            "$NOT", Map.of("skills ARRAY_CONTAINS_ALL", List.of("Typescript", "Java" )), //
                            "age <", 100) //
                    .sort("id", "ASC" ) //
                    .limit(10) //
                    .offset(0);

            // test find
            var users = db.find(host, cond, "Users" ).toList(FullNameUser.class);

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0).id).isEqualTo(user1.id);
            assertThat(users.get(1).id).isEqualTo(user3.id);

            var count = db.count(host, cond, "Users" );
            assertThat(count).isEqualTo(2);
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
    void find_using_not_with_multiple_sub_conds_should_work() throws Exception {
        var partition = "Families";

        {
            // $AND + $NOT + multiple sub conds
            var cond = Condition.filter("lastName", "Andersen", "$NOT CONTAINS", List.of(
                    Condition.filter("address.state", "NY"),
                    Condition.filter("creationDate <", 0)
            ));
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).hasSize(1);
            assertThat(docs.get(0)).containsEntry("id", "AndersenFamily");

        }
    }

    @Test
    void find_with_custom_class_value_should_work() throws Exception {
        var partition = "Families";
        {
            var cond = Condition.filter("lastName", EvalSkip.singleton);
            var docs = db.find(host, cond, partition).toMap();
            assertThat(docs).isEmpty();
        }
    }

    @Test
    void find_with_expression_value_should_work() throws Exception {
        var partition = "Users";

        {
            // both side calculation
            var cond = Condition.filter("$EXPRESSION exp1", "c.age / 10 < ARRAY_LENGTH(c.skills)");
            var users = db.find(host, cond, partition).toList(User.class);
            assertThat(users).hasSizeGreaterThanOrEqualTo(1);
            assertThat(users).anyMatch(user -> user.id.equals("id_find_filter2"));
            assertThat(users).noneMatch(user -> Set.of("id_find_filter1", "id_find_filter3", "id_find_filter4").contains(user.id));
        }

        {
            // using c["age"]
            var cond = Condition.filter("$EXPRESSION exp1", "c[\"age\"] < ARRAY_LENGTH(c.skills) * 10");
            var users = db.find(host, cond, partition).toList(User.class);
            assertThat(users).hasSizeGreaterThanOrEqualTo(1);
            assertThat(users).anyMatch(user -> user.id.equals("id_find_filter2"));
            assertThat(users).noneMatch(user -> Set.of("id_find_filter1", "id_find_filter3", "id_find_filter4").contains(user.id));
        }
    }

    @Test
    void find_to_iterator_should_work_with_filter() throws Exception {

        // test basic find
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            "fullName.first", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var iterator = db.findToIterator(host, cond, "Users").getMapIterator();

            var users = new ArrayList<Map<String, Object>>();
            while(iterator.hasNext()){
                var user = iterator.next();
                users.add(user);
            }

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0).get("id")).isEqualTo(user1.id);
            assertThat(users.get(0).get("_ts")).isInstanceOfSatisfying(Integer.class, i ->
                    assertThat(i.longValue()).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(1.0)));

        }

        // test basic find using OR
        {
            var cond = Condition.filter("fullName.last OR fullName.first =", "Elise" //
                    ).sort("id", "ASC") //
                    .limit(10) //
                    .offset(0);

            var iterator = db.findToIterator(host, cond, "Users");

            var typedIterator = iterator.getTypedIterator(FullNameUser.class);
            var users = new ArrayList<FullNameUser>();
            while(typedIterator.hasNext()){
                var user = typedIterator.next();
                users.add(user);
            }


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

            var iterator = db.findToIterator(host, cond, "Users");

            var typedIterator = iterator.getTypedIterator(FullNameUser.class);
            var users = new ArrayList<FullNameUser>();
            while(typedIterator.hasNext()){
                var user = typedIterator.next();
                users.add(user);
            }

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

            var iterator = db.findToIterator(host, cond, "Users");

            var users = new ArrayList<FullNameUser>();
            while(iterator.hasNext()){
                var user = iterator.next(FullNameUser.class);
                users.add(user);
            }

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
            var iterator = db.findToIterator(host, cond, "Users");

            var users = new ArrayList<FullNameUser>();
            while(iterator.hasNext()){
                var user = iterator.next(FullNameUser.class);
                users.add(user);
            }

            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0)).hasToString(user2.toString());

        }

        // test limit find
        {
            var users = db.find(host, Condition.filter().sort("_ts", "DESC").limit(2), "Users")
                    .toList(FullNameUser.class);
            assertThat(users.size()).isEqualTo(2);
            assertThat(users.get(0)).hasToString(user3.toString());

            var iterator = db.findToIterator(host, Condition.filter().sort("_ts", "DESC").limit(2), "Users");
            var maps = new ArrayList<Map<String, Object>>();
            while(iterator.hasNext()){
                maps.add(iterator.next(Map.class));
            }
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
            var iterator = db.findToIterator(host, cond, "Users");
            var users = new ArrayList<FullNameUser>();
            while(iterator.hasNext()){
                var user = iterator.next(FullNameUser.class);
                users.add(user);
            }

            assertThat(users.size()).isEqualTo(1);
            assertThat(users.get(0)).hasToString(user2.toString());

            // count
            var count = db.count(host, cond, "Users");
            assertThat(count).isEqualTo(1);
        }
    }

    @Test
    void find_to_iterator_should_work_for_array_contains_any_field_query() throws Exception {
        var partition = "Families";

        {
            // ARRAY_CONTAINS_ANY
            // children is an array, and grade is a field of children
            var cond = Condition.filter("children ARRAY_CONTAINS_ANY grade", List.of(5, 8));
            var iterator = db.findToIterator(host, cond, partition);

            var docs = new ArrayList<Map<String, Object>>();
            while(iterator.hasNext()){
                var doc = iterator.next().toMap();
                docs.add(doc);
            }
            assertThat(docs).hasSize(2);
        }

        {
            // ARRAY_CONTAINS_ALL
            // children is an array, and grade is a field of children
            var cond = Condition.filter("children ARRAY_CONTAINS_ALL grade", List.of(1, 8));
            var iterator = db.findToIterator(host, cond, partition);
            var docs = new ArrayList<Map<String, Object>>();
            while(iterator.hasNext()){
                var doc = iterator.next().toMap();
                docs.add(doc);
            }
            assertThat(docs).hasSize(1);
        }
    }

    @Test
    void fields_with_empty_field_should_work() throws Exception {
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
    void regex_should_work_with_filter() throws Exception {

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
    void field_a_equals_field_b_should_work_with_filter() throws Exception {

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

            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(2);

            var expect = Map.of("Hanks", 2, "Henry", 1);

            var lastName1 = result.get(0).getOrDefault("last", "").toString();
            // the result of count should be integer
            assertThat(result.get(0).get("facetCount")).isInstanceOf(Integer.class).isEqualTo(expect.get(lastName1));

            var lastName2 = result.get(1).getOrDefault("last", "").toString();
            assertThat(result.get(1).get("facetCount")).isEqualTo(expect.get(lastName2));

        }

        // test aggregate(count specific field)
        {
            var aggregate = Aggregate.function("count(c.fullName.last) AS lastNameCount").groupBy("age")
                    .conditionAfterAggregate(Condition.filter().sort("age", "ASC"));

            var result = db.aggregate(host, aggregate, "Users").toMap();
            assertThat(result).hasSize(3);

            var expect = Map.of("12", 1,  "30", 1, "45", 1);

            var age1 = result.get(0).getOrDefault("age", "").toString();

            // the result of count should be integer
            assertThat(result.get(0).get("lastNameCount")).isInstanceOf(Integer.class).isEqualTo(expect.get(age1));

            var age2 = result.get(1).getOrDefault("age", "").toString();
            assertThat(result.get(1).get("lastNameCount")).isEqualTo(expect.get(age2));

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
    void aggregate_should_work_grouping_by_id() throws Exception {
        // test aggregate(group by "id")
        {
            var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("id");
            var cond = Condition.filter("id STARTSWITH", "id_find_filter");

            var result = db.aggregate(host, aggregate, cond, "Users").toMap();
            assertThat(result).hasSize(3);

            // the result of count should be integer
            assertThat(result.get(0).get("facetCount")).isInstanceOf(Integer.class);

            assertThat(result.get(0).get("id")).isInstanceOfSatisfying(String.class, (id) ->{
                    assertThat(id).startsWith("id_find_filter");
            });
        }

        // test aggregate(aggregate using "c.id", group by "id")
        {
            var aggregate = Aggregate.function("c.id AS userId").groupBy("id");
            var cond = Condition.filter("id STARTSWITH", "id_find_filter");

            var result = db.aggregate(host, aggregate, cond, "Users").toMap();
            assertThat(result).hasSize(3);

            // we alias id as userId
            assertThat(result.get(0).get("id")).isNull();

            assertThat(result.get(0).get("userId")).isInstanceOfSatisfying(String.class, (id) ->{
                assertThat(id).startsWith("id_find_filter");
            });
        }

        // test aggregate(aggregate using c["id"], group by "id")
        {
            var aggregate = Aggregate.function("c[\"id\"] AS userId").groupBy("id");
            var cond = Condition.filter("id STARTSWITH", "id_find_filter");

            var result = db.aggregate(host, aggregate, cond, "Users").toMap();
            assertThat(result).hasSize(3);

            // we alias id as userId
            assertThat(result.get(0).get("id")).isNull();

            assertThat(result.get(0).get("userId")).isInstanceOfSatisfying(String.class, (id) ->{
                assertThat(id).startsWith("id_find_filter");
            });
        }
    }

    @Test
    void aggregate_should_work_using_min_for_texts() throws Exception {

        var partition = "Users2";
        var id1 = "using_min_for_texts1_" + RandomStringUtils.randomAlphanumeric(3);
        var id2 = "using_min_for_texts2_" + RandomStringUtils.randomAlphanumeric(3);
        var id3 = "using_min_for_texts3_" + RandomStringUtils.randomAlphanumeric(3);

        try {

            var data1 = Map.of("id", id1, "timeSeries", "2024-01-01");
            var data2 = Map.of("id", id2, "timeSeries", "2025-01-01");
            var data3 = Map.of("id", id3, "timeSeries", "2026-04-03");

            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);
            db.upsert(host, data3, partition);

            {
                // test aggregate(min for texts)
                var aggregate = Aggregate.function("min(c.timeSeries) AS result");
                var cond = Condition.filter("id STARTSWITH", "using_min_for_texts");

                var result = db.aggregate(host, aggregate, cond, partition).toMap();
                assertThat(result).hasSize(1);

                assertThat(result.get(0).get("result")).isEqualTo("2024-01-01");
            }


        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
            db.delete(host, id3, partition);
        }

    }

    @Test
    void aggregate_should_work_grouping_by_array() throws Exception {

        var partition = "Users2";
        var id1 = "grouping_by_array1_" + RandomStringUtils.randomAlphanumeric(3);
        var id2 = "grouping_by_array2_" + RandomStringUtils.randomAlphanumeric(3);
        var id3 = "grouping_by_array3_" + RandomStringUtils.randomAlphanumeric(3);
        var id4 = "grouping_by_array4_" + RandomStringUtils.randomAlphanumeric(3);

        try {

            var data1 = Map.of("id", id1, "orgIds", List.of("org1", "org2"));
            var data2 = Map.of("id", id2, "orgIds", List.of("org1", "org2"));
            var data3 = Map.of("id", id3, "orgIds", List.of("org1"));
            var data4 = Map.of("id", id4, "orgIds", List.of());


            db.upsert(host, data1, partition);
            db.upsert(host, data2, partition);
            db.upsert(host, data3, partition);
            db.upsert(host, data4, partition);

            {
                // test aggregate(min for texts)
                var aggregate = Aggregate.function("count(1) AS count").groupBy("orgIds")
                        .conditionAfterAggregate(Condition.filter().sort("count", "DESC"));
                var cond = Condition.filter("id STARTSWITH", "grouping_by_array");

                var result = db.aggregate(host, aggregate, cond, partition).toMap();
                assertThat(result).hasSize(3);

                assertThat(result.get(0).get("count")).isEqualTo(2);
                assertThat(result.get(0).get("orgIds")).isEqualTo(List.of("org1", "org2"));
            }


        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
            db.delete(host, id3, partition);
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
    void aggregate_should_work_using_simple_field_without_function() throws Exception {

        // test simple field without function
        {
            var aggregate = Aggregate.function("c['address']['state'] as result");

            var result = db.aggregate(host, aggregate,
                    Condition.filter("lastName", "Andersen"), "Families").toMap();

            assertThat(result).hasSize(1);

            assertThat(result.get(0)).containsEntry("result", "WA");
        }
    }

    @Test
    void aggregate_should_work_with_condition_afterwards() throws Exception {

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
    void aggregate_should_work_with_sum() throws Exception {

        // test sum(c.creationDate)
        {
            var aggregate = Aggregate.function("sum(c.creationDate) as dateSum");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter(), "Families").toMap();
            assertThat(result).hasSize(1);

            var dateSum = result.get(0).get("dateSum");
            if (dateSum instanceof Double) {
                assertThat((Double) dateSum).isGreaterThan(2.8E9);
            } else if (dateSum instanceof Long) {
                assertThat((Long) dateSum).isGreaterThan(2_800_000_000L);
            }

        }
    }

    @Test
        // not supported yet
    void aggregate_should_work_with_array_length() throws Exception {

        // test array_length(c.children)
        {
            var aggregate = Aggregate.function("array_length(c.children) as childrenLength")
                    .conditionAfterAggregate(Condition.filter().sort("childrenLength", "ASC"));
            ;

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter(), "Families").toMap();
            assertThat(result).hasSize(2);

            assertThat(result.get(0).get("childrenLength")).isEqualTo(1);
            assertThat(result.get(1).get("childrenLength")).isEqualTo(3);
        }
    }

    @Test
    void aggregate_should_work_with_nested_functions() throws Exception {

        // test ARRAY_LENGTH(c.area.city.street.rooms)
        {
            var aggregate = Aggregate.function("SUM(ARRAY_LENGTH(c['area']['city']['street']['rooms'])) AS 'count'");

            // test find
            var result = db.aggregate(host, aggregate,
                    Condition.filter(), "Families").toMap();

            assertThat(result).hasSize(1);
            var value = result.get(0).getOrDefault("count", "").toString();
            assertThat(Integer.parseInt(value)).isEqualTo(2);

        }

        // test ARRAY_LENGTH(c.children)
        {
            var aggregate = Aggregate.function("SUM(ARRAY_LENGTH(c.children)) AS facetCount");

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
            // postgres works correctly
            assertThat(result.get(0).get("score")).isInstanceOf(Double.class).isEqualTo(10d);

            assertThat(result.get(0).get("_ts")).isInstanceOf(Integer.class);


        } finally {
            db.delete(host, id, partition);
        }
    }


    @Test
    void find_should_work_with_join() throws Exception {

        // query with join
        {
            var cond = new Condition();

            {
                // returnAllSubArray false
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
            }

            {
                // returnAllSubArray true
                cond = Condition.filter("area.city.street.rooms.no", "001") //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("area.city.street.rooms"))
                        .returnAllSubArray(true);
                ;

                var result = db.find(host, cond, "Families").toMap();
                assertThat(result).hasSize(1);
                var rooms = JsonUtil.toListOfMap(JsonUtil.toJson(JsonUtil.toMap(JsonUtil.toMap(JsonUtil.toMap(result.get(0).get("area")).get("city")).get("street")).get("rooms")));
                assertThat(rooms).hasSize(2);
                assertThat(rooms.get(0)).containsEntry("no", "001");
                assertThat(rooms.get(1)).containsEntry("no", "002");
            }

            {
                // LIKE
                cond = Condition.filter("area.city.street.rooms.no LIKE", "%01") //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("area.city.street.rooms"))
                        .returnAllSubArray(true);
                ;

                var result = db.find(host, cond, "Families").toMap();
                assertThat(result).hasSize(1);
                var rooms = JsonUtil.toListOfMap(JsonUtil.toJson(JsonUtil.toMap(JsonUtil.toMap(JsonUtil.toMap(result.get(0).get("area")).get("city")).get("street")).get("rooms")));
                assertThat(rooms).hasSize(2);
                assertThat(rooms.get(0)).containsEntry("no", "001");
                assertThat(rooms.get(1)).containsEntry("no", "002");
            }

            {
                // children.grade < 6
                cond = Condition.filter("parents.firstName", "Thomas", "parents.firstName", "Mary Kay", "children.gender", "female", "children.grade <", 6, "room*no-01.area", 10) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("parents", "children", "room*no-01"))
                        .returnAllSubArray(false);

                var result = db.find(host, cond, "Families").toMap();
                assertThat(result).hasSize(1);
                assertThat(result.get(0)).containsEntry("_partition", "Families");
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).stream().anyMatch(item -> item.get("firstName").toString().equals("Mary Kay"))).isTrue();
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("gender").toString().equals("female"))).isTrue();
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("children"))).stream().anyMatch(item -> item.get("grade").toString().equals("5"))).isTrue();
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("room*no-01"))).stream().anyMatch(item -> item.get("area").toString().equals("10"))).isTrue();
            }
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

    }

    @Test
    void find_should_work_with_join_using_array_contains_any() throws Exception {

        var id1 = "joinTestArrayContainsAnyId";
        var id2 = "joinTestArrayContainsAnyId2";
        var partition = "Users";

        try {
            var user = new User(id1, "firstNameJoin", "lastNameJoin");
            var userMap = JsonUtil.toMap(user);
            userMap.put("rooms", List.of(Map.of("no", List.of(1, 2, 3)), Map.of("no", List.of(2, 4))));
            db.upsert(host, userMap, partition);


            var user2 = new User(id2, "firstNameJoin2", "lastNameJoin2");
            var userMap2 = JsonUtil.toMap(user2);
            userMap2.put("rooms", List.of(Map.of("no", List.of(4, 5, 6)), Map.of("no", List.of(6, 7, 8))));
            db.upsert(host, userMap2, partition);

            {
                // ARRAY_CONTAINS_ANY with join, contains integer
                var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", 3) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("rooms"))
                        .returnAllSubArray(false);

                // test find
                var items = db.find(host, cond, "Users").toMap();

                assertThat(items).hasSize(1);
                assertThat(items.get(0).get("id")).hasToString(id1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asList().contains(3);
            }


            {
                // ARRAY_CONTAINS_ANY with join, List of integers
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
                assertThat(items.get(0).get("id")).hasToString(id1);

            }
        }finally {
            db.delete(host, id1, "Users");
            db.delete(host, id2, "Users");
        }

    }

    @Test
    void find_should_work_with_join_using_array_contains_all() throws Exception {

        // query with join. floors.rooms ARRAY_CONTAINS_ALL name. join:floors
        {
            var user = new User("joinTestArrayContainAll", "firstNameJoin", "lostNameJoin");
            var userMap = JsonUtil.toMap(user);

            var floorsJson = """
                    {
                      "floors":[
                        {
                          "name": "floor1",
                          "rooms": [
                            {
                              "no": 1,
                              "name": "r1"
                            },
                            {
                              "no": 2,
                              "name": "r2"
                            }
                          ]
                        },
                        {
                          "name": "floor2",
                          "rooms": [
                            {
                              "no": 3,
                              "name": "r3"
                            },
                            {
                              "no": 4,
                              "name": "r4"
                            }
                          ]
                        }
                      ]
                    }
                    """;

            userMap.putAll(JsonUtil.toMap(floorsJson));

            try {
                db.upsert(host, userMap, "Users");
                { // returnAllSubArray=true
                    var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ALL name", List.of("r3", "r4")) //
                            .sort("id", "ASC") //
                            .limit(10) //
                            .offset(0)
                            .join(Set.of("floors"))
                            .returnAllSubArray(true);

                    // test find
                    var items = db.find(host, cond, "Users").toMap();

                    assertThat(items).hasSize(1);
                    assertThat(items.get(0).get("id")).hasToString(user.id);
                    assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("floors")))).hasSize(2);
                }

                { // returnAllSubArray=false
                    var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ALL name", List.of("r3", "r4")) //
                            .sort("id", "ASC") //
                            .limit(10) //
                            .offset(0)
                            .join(Set.of("floors"))
                            .returnAllSubArray(false);

                    // test find
                    var items = db.find(host, cond, "Users").toMap();

                    assertThat(items).hasSize(1);
                    assertThat(items.get(0).get("id")).hasToString(user.id);
                    var floors = JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("floors")));
                    assertThat(floors).hasSize(1);
                    assertThat((List)floors.get(0).get("rooms")).hasSize(2);

                }
            } finally {
                db.delete(host, user.id, "Users");
            }
        }

        {
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
                db.delete(host, id1, "Users");
                db.delete(host, id2, "Users");
            }
        }

    }

    @Test
    void find_should_work_with_join_using_array_contains() throws Exception {

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

        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }
    }

    @Test
    void find_should_work_with_join_using_limit_and_fields() throws Exception {

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
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(2);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("givenName", "Robin");
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
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents")))).hasSize(2);
            assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(result.get(0).get("parents"))).get(0)).containsEntry("givenName", "Robin");
            assertThat(result.get(0).get("id")).hasToString("WakefieldFamily");
            assertThat(result.get(0).get("address")).isNotNull();

            // fields excluded
            assertThat(result.get(0).get("children")).isNull();
            assertThat(result.get(0).get("lastName")).isNull();

        }

    }

    @Test
    void find_should_work_with_join_together_with_aggregate() throws Exception {
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
            //cosmosdb will return "not exist" for "lastName" (the "lastName" key does not exist in the document)
            //postgres will return null and be change to Map.of()
            assertThat(result.get(0).getOrDefault("lastName", "")).isInstanceOfSatisfying(Map.class, map -> assertThat(map).isEmpty());
            assertThat(result.get(1).getOrDefault("lastName", "")).isEqualTo("Andersen");
            assertThat(Integer.parseInt(result.get(0).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
            assertThat(Integer.parseInt(result.get(1).getOrDefault("facetCount", "-1").toString())).isEqualTo(1);
        }
    }

    @Test
    void find_should_work_with_join_using_elem_match() throws Exception {

        var id3 = "Adams";
        var family3 = Map.of(
                "id", id3,
                "children", List.of(
                        Map.of(
                                "gender", "male",
                                "grade", 4
                        ),
                        Map.of(
                                "gender", "female",
                                "grade", 8
                        )
                ));


        try {
            db.upsert(host, family3, "Families");

            // condition on the same sub array should be both applied to the element(e.g. children.gender = "female" AND children.grade = 5)
            // we introduced a new SubConditionType like "$ELEM_MATCH" in postgres to achieve this
            { // test returnAllSubArray(true)
                var cond = new Condition();
                cond = Condition.filter(SubConditionType.ELEM_MATCH,
                                Map.of("children.gender", "male",
                                        "children.grade >=", 5)
                        )
                        .sort("lastName", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("parents", "children"))
                        .returnAllSubArray(true)
                ;
                // test find
                var result = db.find(host, cond, "Families").toMap();
                assertThat(result).hasSize(1);
                assertThat(result.get(0).getOrDefault("id", "")).isEqualTo("WakefieldFamily");
                assertThat((List<Map<String, Object>>) result.get(0).get("children")).hasSize(3);
            }
            { // test returnAllSubArray(false)
                var cond = new Condition();
                cond = Condition.filter(SubConditionType.ELEM_MATCH,
                                Map.of("children.gender", "male",
                                        "children.grade >=", 5)
                        )
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
                // only sub set of children is returned
                assertThat((List<Map<String, Object>>) result.get(0).get("children")).hasSize(1);
            }
        }finally {
            db.delete(host, id3, "Families");
        }
    }

    @Test
    void findToIterator_should_work_with_join_using_array_contains() throws Exception {

        // ARRAY_CONTAINS query with join

        var id1 = "joinTestArrayContainId_iterator";
        var id2 = "joinTestArrayContainId2_iterator";
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
                var iterator = db.findToIterator(host, cond, "Users").getTypedIterator(Map.class);

                var items = new ArrayList<Map<String, Object>>();
                while(iterator.hasNext()){
                    items.add(iterator.next());
                }

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId_iterator");
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
                var iterator = db.findToIterator(host, cond, "Users").getMapIterator();

                var items = new ArrayList<Map<String, Object>>();
                while(iterator.hasNext()){
                    items.add(iterator.next());
                }

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId_iterator");

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
                var iterator = db.findToIterator(host, cond, "Users");

                var items = new ArrayList<Map<String, Object>>();
                while(iterator.hasNext()){
                    items.add(iterator.next().toMap());
                }

                assertThat(items).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms")))).hasSize(1);
                assertThat(JsonUtil.toListOfMap(JsonUtil.toJson(items.get(0).get("rooms"))).get(0).get("no")).asInstanceOf(LIST).contains(3);
                assertThat(items.get(0).get("id")).hasToString("joinTestArrayContainId_iterator");

            }

        } finally {
            db.delete(host, id1, partition);
            db.delete(host, id2, partition);
        }
    }
    
    @Disabled
    /**
     * TODO rawSql is not implemented for postgres
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

            var items = db.find(host, cond, partition).toMap();

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

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(2);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
            assertThat(items.get(1).get("creationDate")).hasToString("1431620462");
        }

        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-OR.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(host, cond, partition).toMap();

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

            var items = db.find(host, cond, partition).toMap();

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

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-AND.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // using json to represent a filter (in order to support rest api 's parameter)
            var partition = "Families";

            var filter = JsonUtil.toMap(CosmosDatabaseTest.class.getResourceAsStream("familyQuery-AND2.json"));
            var cond = new Condition(filter).sort("id", "ASC");

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }

    }

    @Test
    void sub_cond_query_should_work_4_empty_list() throws Exception {
        {
            // AND with empty list
            var partition = "Families";

            var cond = Condition.filter("id", "AndersenFamily", AND, List.of(
                            Condition.filter("$AND sub cond", List.of()))
                    )
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }

        {
            // OR with empty list
            var partition = "Families";

            var cond = Condition.filter("id", "AndersenFamily", OR, List.of(
                            Condition.filter("$OR sub cond", List.of()))
                    )
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }

        {
            // OR with empty list
            var partition = "Families";

            var cond = Condition.filter("id", "AndersenFamily", AND, List.of(
                            Condition.filter("$NOT sub cond", List.of()))
                    )
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();

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

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("WakefieldFamily");
        }

    }

    @Test
    void sub_cond_query_should_work_when_subquery_is_null_or_empty() throws Exception {
        {
            // $AND null
            var partition = "Families";

            // null will be ignored
            var cond = Condition.filter("lastName", "Andersen", SubConditionType.AND, null, "$AND 2", List.of())
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();
            assertThat(items).hasSize(1);
            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // $OR null
            var partition = "Families";

            // null will be ignored
            var cond = Condition.filter("lastName", "Andersen", SubConditionType.OR, null, "$OR 2", List.of())
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
        {
            // $NOT null
            var partition = "Families";

            // null will be ignored
            var cond = Condition.filter("lastName", "Andersen", SubConditionType.NOT, null, "$NOT 2", List.of())
                    .sort("id", "ASC") //
                    ;

            var items = db.find(host, cond, partition).toMap();

            assertThat(items).hasSize(1);

            assertThat(items.get(0).get("id")).hasToString("AndersenFamily");
        }
    }

    @Test
    void sort_with_createAt_should_work() throws Exception {
        var partition = "UserSorts";
        int size = 2;
        var userList = new ArrayList<User>(size);
        var createdAt = Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_DATE);
        for (int i = 0; i < size; i++) {
            userList.add(new User("sort_with_createAt_should_work" + i, "testFirstName" + i, "testLastName" + i, createdAt));
        }

        try {

            db.upsert(host, userList.get(0), partition);
            // let _ts be different
            Thread.sleep(1);
            db.upsert(host, userList.get(1), partition);

            var users = db.find(host, Condition.filter("id LIKE", "sort_with_createAt_should_work%")
                    .sort("createAt", "DESC"), partition).toList(User.class);

            assertThat(users).hasSize(2);

            // check the sort result is correct (DESC)
            assertThat(users.get(0).id).isEqualTo(userList.get(1).id);

        } finally {
            db.batchDelete(host, userList, partition);
        }
    }

    @Test
    void dynamic_field_and_is_defined_should_work() throws Exception {
        var partition = "SheetContents";

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
        var partition = "SheetContents2";

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

                // assert that in raw doc in db, the _ts is Double
                var dataSource = ((PostgresImpl) db.getCosmosAccount()).getDataSource();

                try(var conn = dataSource.getConnection()) {
                    var doc = TableUtil.readRecord(conn, host, partition, id);
                    assertThat((Double) doc.data.get("_ts")).isInstanceOf(Double.class).isCloseTo(Instant.now().getEpochSecond(), Percentage.withPercentage(0.01));
                }

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
                            .remove("/skills/2") // remove an array item at index 2
                            ;
                    var item = db.patch(host, id, operations, partition).toMap();
                    assertThat(item.get("score")).isNull();
                    assertThat((List<String>) item.get("skills")).hasSize(3);
                    assertThat(((List<String>) item.get("skills")).get(2)).isEqualTo("TypeScript");

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
    void patch_should_work_with_nested_pojo() throws Exception {
        var partition = "PatchPojoTests";
        var id = "patch_should_work_with_nested_pojo";

        try {
            var data1 = Map.of("id", id, "name", "John",
                    "contents", Map.of("check1", new CheckBox("id1", "name1", CheckBox.Align.VERTICAL)),
                    "score", 85.5);
            db.upsert(host, data1, partition).toMap();

            {
                // Set should work

                var operations = PatchOperations.create()
                        .set("/contents", Map.of(
                                "check1", new CheckBox("id1", "name1", CheckBox.Align.HORIZONTAL),
                                "check2", new CheckBox("id2", "name2", CheckBox.Align.VERTICAL)
                        )); // reset contents to a new list
                var item = db.patch(host, id, operations, partition).toMap();

                var contents = (Map<String, Object>) item.get("contents");
                assertThat(contents).hasSize(2);

                assertThat((Map<String, Object>) contents.get("check1"))
                        .containsEntry("id", "id1")
                        .containsEntry("align", CheckBox.Align.HORIZONTAL.name());

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
                assertThat((Map<String, Object>) inc1.get("contents")).containsEntry("age", 21);

                // increment by -3, integer field
                var inc2 = db.increment(host, id, "/contents/age", -3, partition).toMap();
                assertThat((Map<String, Object>) inc2.get("contents")).containsEntry("age", 18);

                // increment by 1, long field
                var inc3 = db.increment(host, id, "/number", 1, partition).toMap();
                assertThat(inc3).containsEntry("number", 3_147_483_648L);

                // increment by 5, double field
                var inc4 = db.increment(host, id, "/score", 5, partition).toMap();
                assertThat(inc4).containsEntry("score", 90.5);

            }

            {
                // when incrementing a string field, nothing will change for postgres
                var incStr = db.increment(host, id, "/name", 5, partition).toMap();
                assertThat(incStr).containsEntry("name", "John");  // unchanged
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

    @Test
    void addExpireAt_addEtag_should_work() {
        var mdb = (PostgresDatabaseImpl) db;

        { // expireAt should not be added if "ttl" does not exist

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1"));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNull();
            assertThat(map).doesNotContainKey(PostgresDatabaseImpl.EXPIRE_AT);

        }

        { // expireAt should not be added if "ttl" is null

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1"));
            map.put("ttl", null);

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNull();
            assertThat(map).doesNotContainKey(PostgresDatabaseImpl.EXPIRE_AT);

        }

        { // expireAt should not be added if "ttl" cannot be parsed to int

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1", "ttl", "invalid format"));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNull();
            assertThat(map).doesNotContainKey(PostgresDatabaseImpl.EXPIRE_AT);

        }

        { // expireAt should be added if "ttl" is set to integer

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1", "ttl", 60));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNotNull().isBetween(Instant.now().plusSeconds(59), Instant.now().plusSeconds(61));
            assertThat(map).containsKey(PostgresDatabaseImpl.EXPIRE_AT);
            assertThat(map.get(PostgresDatabaseImpl.EXPIRE_AT)).isEqualTo(expireAt.getTime() / 1000L);

        }

        { // expireAt should not be added if "ttl" is not integer

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1", "ttl", 60L));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNull();
            assertThat(map).doesNotContainKey(PostgresDatabaseImpl.EXPIRE_AT);

        }

        { // expireAt should be added if "ttl" is 30 days(which is a large value, if represent in milliseconds)

            var thirtyDaysInSeconds = 30 * 24 * 60 * 60;
            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1", "ttl", thirtyDaysInSeconds));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNotNull().isBetween(Instant.now().plusSeconds(thirtyDaysInSeconds), Instant.now().plusSeconds(thirtyDaysInSeconds + 1));
            assertThat(map.get(PostgresDatabaseImpl.EXPIRE_AT)).isEqualTo(expireAt.getTime()/1000L);

        }

        { // expireAt should be added if "ttl" * 1000 is larger than Integer.MAX_VALUE

            var longPeriod = (Integer.MAX_VALUE - 1) / 1000;
            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1", "ttl", longPeriod));

            var expireAt = mdb.addExpireAt(map);

            assertThat(expireAt).isNotNull().isBetween(Instant.now().plusSeconds(longPeriod), Instant.now().plusSeconds(longPeriod + 1));
            assertThat(map.get(PostgresDatabaseImpl.EXPIRE_AT)).isEqualTo(expireAt.getTime()/1000L);

        }

        { // etag should be added

            Map<String, Object> map = new LinkedHashMap<>(Map.of("id", "id1"));
            var etag = mdb.addEtag4(map);

            assertThat(etag).isNotEmpty();
            assertThat(map).containsKey(TableUtil.ETAG);

            // to check if it can be parsed as a UUID:
            assertThatCode(() -> UUID.fromString(etag)).doesNotThrowAnyException();

        }
    }

    @Test
    void expireAt_should_be_added_if_ttl_is_set() throws Exception {
        var partition = "TTLTests";

        var id = "expireAt_should_be_added_if_ttl_is_set" + RandomStringUtils.randomAlphanumeric(8);

        try {

            { // create
                var data1 = Map.of("id", id, "ttl", 0);
                var result = db.create(host, data1, partition).toMap();

                assertThat(result).containsKey("_expireAt");
                var expireAt = new Date(((Integer)result.get("_expireAt")).longValue() * 1000);
                assertThat(expireAt).isBetween(Instant.now().minusSeconds(1), Instant.now().plusSeconds(1));

            }

            { // upsert
                var data1 = Map.of("id", id, "ttl", 0);
                var result = db.upsert(host, data1, partition).toMap();

                assertThat(result).containsKey("_expireAt");
                var expireAt = new Date(((Integer)result.get("_expireAt")).longValue() * 1000);
                assertThat(expireAt).isBetween(Instant.now().minusSeconds(1), Instant.now().plusSeconds(1));
            }

            { // update
                var data1 = Map.of("id", id, "ttl", 60);
                var result = db.update(host, data1, partition).toMap();

                assertThat(result).containsKey("_expireAt");
                var expireAt = new Date(((Integer)result.get("_expireAt")).longValue() * 1000);
                assertThat(expireAt).isBetween(Instant.now().plusSeconds(59), Instant.now().plusSeconds(61));

            }


        } finally {
            db.delete(host, id, partition);
        }

    }

    @Disabled
    void expireAt_should_be_deleted_after_1min() throws Exception {
        var partition = "TTLDeleteTests";

        var id = "expireAt_should_be_deleted_after_1min" + RandomStringUtils.randomAlphanumeric(8);

        try {
            db.enableTTL(host, partition, 1);

            { // create
                var data1 = Map.of("id", RandomStringUtils.randomAlphanumeric(8), "ttl", 1);
                var result = db.create(host, data1, partition).toMap();

                assertThat(result).containsKey("_expireAt");
                assertThat((Date) result.get("_expireAt")).isBetween(Instant.now().minusSeconds(1), Instant.now().plusSeconds(1));

                // ttl = 1s, interval is 60s, so it will be deleted after 62s
                Thread.sleep(62 * 1000);

                // assertThat the record is deleted
                var read = db.readSuppressing404(host, id, partition);
                assertThat(read).isNull();
            }


        } finally {
            db.delete(host, id, partition);
            db.disableTTL(host, partition);
        }

    }


    @Test
    void etag_should_be_added() throws Exception {
        var partition = "EtagTests";

        var id = "etag_should_be_added" + RandomStringUtils.randomAlphanumeric(8);
        try {

            { // create
                var data1 = Map.of("id", id);
                var result = db.create(host, data1, partition).toMap();

                assertThat(result).containsKey(TableUtil.ETAG);
                assertThat((String) result.get(TableUtil.ETAG)).isNotEmpty();

            }

            { // upsert
                var data1 = Map.of("id", id, "score", 0);
                var result = db.upsert(host, data1, partition).toMap();

                assertThat(result).containsKey(TableUtil.ETAG);
                assertThat((String) result.get(TableUtil.ETAG)).isNotEmpty();
            }

            { // update
                var data1 = Map.of("id", id, "ttl", 60);
                var result = db.update(host, data1, partition).toMap();

                assertThat(result).containsKey(TableUtil.ETAG);
                assertThat((String) result.get(TableUtil.ETAG)).isNotEmpty();

            }


        } finally {
            db.delete(host, id, partition);
        }

    }

    @Test
    void enableTTL_disableTTL_should_work() throws Exception {
        var partition = "Families";

        try {
            var jobName = db.enableTTL(host, partition);
            assertThat(jobName).isEqualTo(TTLUtil.getJobName(host, partition));

            var disableResult = db.disableTTL(host, partition);
            assertThat(disableResult).isEqualTo(true);

        } finally {
            try(var conn = db.dataSource.getConnection()){
                TTLUtil.unScheduleJob(conn, host, partition);
            }
        }
    }

    @Test
    void enableTTL_should_work_for_not_exist_table() throws Exception {
        var partition = "TableNotExist" + RandomStringUtils.randomAlphanumeric(8);

        try {
            var jobName = db.enableTTL(host, partition);
            assertThat(jobName).isEqualTo(TTLUtil.getJobName(host, partition));

            var disableResult = db.disableTTL(host, partition);
            assertThat(disableResult).isEqualTo(true);

        } finally {
            try(var conn = db.dataSource.getConnection()){
                TTLUtil.unScheduleJob(conn, host, partition);
            }
        }
    }


    /**
     * test enableTTL when immediately after table created, which is possible in a production environment
     * @throws Exception
     */
    @Test
    void enableTTL_should_work_when_immediately_after_table_created_multi_thread_multi_connection() throws Exception {

        var tableNamePrefix = "schedule_job_multi_thread_multi_conn_test";
        int threadCount = 100;
        List<String> results = new ArrayList<>();

        try {
            List<Future<String>> futures = new ArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                futures.add(SINGLE_TASK_EXECUTOR.submit(() -> {
                    try {
                        var tableName = tableNamePrefix + idx;
                        create_and_test_enableTTL(tableName);
                        return tableName;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (int i = 0; i < threadCount; i++) {
                results.add(futures.get(i).get());
            }

            assertThat(results).hasSize(threadCount);

        } finally {
            try (var conn = db.getDataSource().getConnection()) {
                for (int i = 0; i < threadCount; i++) {
                    var tableName = tableNamePrefix + i;
                    TTLUtil.unScheduleJob(conn, host, tableName);
                    TableUtil.dropTableIfExists(conn, host, tableName);
                }
            }
        }
    }

    /**
     * test helper method. create table, schedule job, un-schedule job using different connection
     * @param tableName
     * @return
     * @throws Exception
     */
    String create_and_test_enableTTL(String tableName) throws Exception {

        try (var conn = db.getDataSource().getConnection()) {
            // create table
            TableUtil.createTableIfNotExists(conn, host, tableName);
        }

        // schedule job
        var jobName= db.enableTTL(host, tableName);
        assertThat(jobName).isNotEmpty();

        try (var conn = db.getDataSource().getConnection()) {

            // check whether the job exists
            assertThat(TTLUtil.jobExists(conn, host, tableName)).isTrue();

            // un-schedule job
            var unScheduled = TTLUtil.unScheduleJob(conn, host, tableName);
            assertThat(unScheduled).isTrue();

            // check whether the job exists
            assertThat(TTLUtil.jobExists(conn, host, tableName)).isFalse();

            return jobName;

        } finally {
            try (var conn = db.getDataSource().getConnection()) {
                TTLUtil.unScheduleJob(conn, host, tableName);
                TableUtil.dropTableIfExists(conn, host, tableName);
            }
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

    static void initData4ComplexQuery() throws Exception {
        user1 = new FullNameUser("id_find_filter1", "Elise", "Hanks", 12, "test1@example.com", "2020-10-01", "Blanco");
        user2 = new FullNameUser("id_find_filter2", "Matt", "Hanks", 30, "Test2@example.com", "2020-11-01", "Typescript", "Javascript", "React", "Java");
        user3 = new FullNameUser("id_find_filter3", "Tom", "Henry", 45,  "test3@example.com", "2020-12-01", "Java", "Go", "Python");
        user4 = new FullNameUser("id_find_filter4", "Andy", "Henry", 45,  "Test4@example.com", "2020-12-01", "Javascript", "Java");

        // prepare
        db.upsert(host, user1, "Users");
        Thread.sleep(1); // let "_ts" be different for each record
        db.upsert(host, user2, "Users");
        Thread.sleep(1);
        db.upsert(host, user3, "Users");
        Thread.sleep(1);
        // different partition
        db.upsert(host, user4, "Users2");
    }

}
