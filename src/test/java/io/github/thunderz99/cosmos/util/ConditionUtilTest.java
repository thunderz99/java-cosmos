package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.mongodb.client.model.Filters;
import io.github.thunderz99.cosmos.condition.Condition;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConditionUtilTest {

    @Test
    public void equalsOperator_should_work() {
        {
            // with =
            Map<String, Object> filter = Map.of("id =", "id010");
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("id", "id010").toBsonDocument());
        }
        {
            // without =
            Map<String, Object> filter = Map.of("id", "id010");
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("id", "id010").toBsonDocument());
        }

    }

    @Test
    public void equalsOperator_should_work_for_list() {
        // "id" without "=", followed by a List, means IN
        Map<String, Object> filter = Map.of("id", List.of("id010", "id011", "id012"));
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.in("id", List.of("id010", "id011", "id012")).toBsonDocument());
    }


    @Test
    public void notEqualsOperator_should_work() {
        Map<String, Object> filter = Map.of("firstName != ", "Andy");
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("firstName", new Document("$ne", "Andy")).toBsonDocument());
    }

    @Test
    public void greaterThanOrEqualsOperator_should_work() {
        Map<String, Object> filter = Map.of("age >=", 20);
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("age", new Document("$gte", 20)).toBsonDocument());
    }

    @Test
    public void likeOperator_should_work() {
        {
            // using %
            Map<String, Object> filter = Map.of("firstName LIKE ", "%dy%");
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.regex("firstName", ".*dy.*").toBsonDocument());
        }
        {
            // using _
            Map<String, Object> filter = Map.of("firstName LIKE ", "_uc_");
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.regex("firstName", ".uc.").toBsonDocument());
        }

    }

    @Test
    public void startsWithOperator_should_work() {
        Map<String, Object> filter = Map.of("firstName STARTSWITH ", "An");
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.regex("firstName", "^" + Pattern.quote("An")).toBsonDocument());
    }

    @Test
    public void endsWithOperator_should_work() {
        Map<String, Object> filter = Map.of("lastName ENDSWITH ", "son");
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.regex("lastName", Pattern.quote("son") + "$").toBsonDocument());
    }

    @Test
    public void containsOperator_should_work() {
        Map<String, Object> filter = Map.of("lastName CONTAINS ", "Will");
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.regex("lastName", ".*" + Pattern.quote("Will") +".*").toBsonDocument());
    }

    @Test
    public void arrayContainsOperator_should_work() {
        Map<String, Object> filter = Map.of("tags ARRAY_CONTAINS", "Java");
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        // eq should do the work
        assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.eq("tags", "Java").toBsonDocument());
    }

    @Test
    public void arrayContainsAnyOperator_should_work() {
        Map<String, Object> filter = Map.of("tags ARRAY_CONTAINS_ANY", List.of("Java", "React"));
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("tags", new Document("$in", List.of("Java", "React"))).toBsonDocument());
    }

    @Test
    public void arrayContainsAllOperator_should_work() {
        Map<String, Object> filter = Map.of("tags ARRAY_CONTAINS_ALL", List.of("Java", "React"));
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("tags", new Document("$all", List.of("Java", "React"))).toBsonDocument());
    }

    @Test
    public void orOperator_should_work() {
        Map<String, Object> filter = Map.of(
                "$OR", List.of(
                        Map.of("position", "leader"),
                        Map.of("organization.id", "executive_committee")
                )
        );
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("$or", List.of(
                new Document("position", "leader"),
                new Document("organization.id", "executive_committee")
        )).toBsonDocument());
    }

    @Test
    public void andOperator_should_work() {
        Map<String, Object> filter = Map.of(
                "$AND", List.of(
                        Map.of("city", "Tokyo"),
                        Map.of("country", "Japan")
                )
        );
        var bsonFilter = ConditionUtil.toBsonFilter(filter);

        assertThat(bsonFilter.toBsonDocument()).isEqualTo(new Document("$and", List.of(
                new Document("city", "Tokyo"),
                new Document("country", "Japan")
        )).toBsonDocument());
    }

    @Test
    public void notOperator_should_work() {
        {
            // not with single sub filter
            Map<String, Object> filter = Map.of(
                    "$NOT", Map.of("lastName CONTAINS", "Willington")
            );
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.nor(Filters.regex("lastName", ".*" + Pattern.quote("Willington") + ".*")).toBsonDocument());
        }
        {
            // not with multiple sub filters
            Map<String, Object> filter = Map.of(
                    "$NOT", List.of(
                            Map.of("lastName", "Willington"),
                            Map.of("age >=", 20)
                    )
            );
            var bsonFilter = ConditionUtil.toBsonFilter(filter);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.nor(Filters.eq("lastName", "Willington"), Filters.gte("age", 20)).toBsonDocument());
        }

        {
            // not in Condition
            var cond = Condition.filter("lastName", "Willington", "age >=", 20).not();
            var bsonFilter = ConditionUtil.toBsonFilter(cond);

            assertThat(bsonFilter.toBsonDocument()).isEqualTo(Filters.nor(Filters.and(Filters.eq("lastName", "Willington"), Filters.gte("age", 20))).toBsonDocument());
        }

    }

    @Test
    public void singleFieldDescendingSort_should_work() {
        List<String> sort = List.of("id", "DESC");
        var bsonSort = ConditionUtil.toBsonSort(sort);

        assertThat(bsonSort.toBsonDocument()).isEqualTo(new Document("id", -1).toBsonDocument());
    }

    @Test
    public void singleFieldAscendingSort_should_work() {
        List<String> sort = List.of("_ts", "ASC");
        var bsonSort = ConditionUtil.toBsonSort(sort);

        assertThat(bsonSort.toBsonDocument()).isEqualTo(new Document("_ts", 1).toBsonDocument());
    }

    @Test
    public void singleFieldWithDefaultAscendingSort_should_work() {
        List<String> sort = List.of("age");
        var bsonSort = ConditionUtil.toBsonSort(sort);

        assertThat(bsonSort.toBsonDocument()).isEqualTo(new Document("age", 1).toBsonDocument());
    }

    @Test
    public void multipleFieldsSort_should_work() {
        List<String> sort = List.of("id", "DESC", "_ts", "ASC", "age");
        var bsonSort = ConditionUtil.toBsonSort(sort);

        Document expectedDocument = new Document()
                .append("id", -1)
                .append("_ts", 1)
                .append("age", 1);

        assertThat(bsonSort.toBsonDocument()).isEqualTo(expectedDocument.toBsonDocument());
    }

    @Test
    public void emptySort_should_return_emptyDocument() {
        List<String> sort = List.of();
        var bsonSort = ConditionUtil.toBsonSort(sort);

        assertThat(bsonSort).isNull();
    }

    @Test
    public void nullSort_should_return_null() {
        var bsonSort = ConditionUtil.toBsonSort(null);
        assertThat(bsonSort).isNull();
    }

}
