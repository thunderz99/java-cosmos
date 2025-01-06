package io.github.thunderz99.cosmos.util;

import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapUtilTest {

    public enum Status {
        info, warn, error,
    }

    public static class User {
        public String name;
        public int age;

        public User() {
        }

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        @Override
        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public boolean equals(Object obj2) {
            if (obj2 == null) {
                return false;
            }

            return this.toString().equals(obj2.toString());
        }
    }

    @Test
    void toFlatMap_should_work() throws Exception {

        {
            // irregular
            assertThat(MapUtil.toFlatMap(null)).isNull();
            assertThat(MapUtil.toFlatMap(Map.of())).isEmpty();
        }
        {
            // primitives
            assertThat(MapUtil.toFlatMap(Map.of("integer", 2))).hasSize(1).containsEntry("/integer", 2);
            assertThat(MapUtil.toFlatMap(Map.of("double.next", 2.0d))).hasSize(1).containsEntry("/double.next", 2.0d);
            assertThat(MapUtil.toFlatMap(Map.of("float", 2.0f))).hasSize(1).containsEntry("/float", 2.0f);
            assertThat(MapUtil.toFlatMap(Map.of("long", 2L))).hasSize(1).containsEntry("/long", 2L);
        }
        {
            //enum
            assertThat(MapUtil.toFlatMap(Map.of("enum", Status.warn))).hasSize(1).containsEntry("/enum", Status.warn);
        }
        {
            //collections
            assertThat(MapUtil.toFlatMap(Map.of("list", List.of(1, "2")))).hasSize(1).containsEntry("/list", List.of(1, "2"));
            assertThat(MapUtil.toFlatMap(Map.of("set", Set.of(2.0f, 3.0d)))).hasSize(1).containsEntry("/set", Set.of(2.0f, 3.0d));
            var queue = Queues.newArrayDeque(List.of(1, 2));
            assertThat(MapUtil.toFlatMap(Map.of("queue", queue))).hasSize(1).containsEntry("/queue", queue);

        }
        {
            //class
            assertThat(MapUtil.toFlatMap(Map.of("user", new User("Tom", 20)))).hasSize(1).containsEntry("/user", new User("Tom", 20));
        }

        {
            //nested maps
            var map = Map.of("id", "ID001",
                    "contents", Map.of("name", "Tom", "age", 20));

            assertThat(MapUtil.toFlatMap(map)).hasSize(3).containsEntry("/id", "ID001")
                    .containsEntry("/contents/name", "Tom")
                    .containsEntry("/contents/age", 20);

        }


    }

    @Test
    void toFlatMapWithPeriod_should_work() throws Exception {

        {
            // irregular
            assertThat(MapUtil.toFlatMapWithPeriod(null)).isNull();
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of())).isEmpty();
        }
        {
            // primitives
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("integer", 2))).hasSize(1).containsEntry("integer", 2);
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("double.next", 2.0d))).hasSize(1).containsEntry("double.next", 2.0d);
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("float", 2.0f))).hasSize(1).containsEntry("float", 2.0f);
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("long", 2L))).hasSize(1).containsEntry("long", 2L);
        }
        {
            //enum
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("enum", Status.warn))).hasSize(1).containsEntry("enum", Status.warn);
        }
        {
            //collections
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("list", List.of(1, "2")))).hasSize(1).containsEntry("list", List.of(1, "2"));
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("set", Set.of(2.0f, 3.0d)))).hasSize(1).containsEntry("set", Set.of(2.0f, 3.0d));
            var queue = Queues.newArrayDeque(List.of(1, 2));
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("queue", queue))).hasSize(1).containsEntry("queue", queue);

        }
        {
            //class
            assertThat(MapUtil.toFlatMapWithPeriod(Map.of("user", new User("Tom", 20)))).hasSize(1).containsEntry("user", new User("Tom", 20));
        }

        {
            //nested maps
            var map = Map.of("id", "ID001",
                    "contents", Map.of("name", "Tom", "age", 20));

            assertThat(MapUtil.toFlatMapWithPeriod(map)).hasSize(3).containsEntry("id", "ID001")
                    .containsEntry("contents.name", "Tom")
                    .containsEntry("contents.age", 20);

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

        var merged = MapUtil.merge(map1, map2);

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
    void isImmutableMap_should_work() {
        assertThat(MapUtil.isImmutableMap(null)).isFalse();
        assertThat(MapUtil.isImmutableMap(Map.of())).isTrue();
        assertThat(MapUtil.isImmutableMap(Collections.singletonMap("a", "b"))).isTrue();
        assertThat(MapUtil.isImmutableMap(Collections.unmodifiableMap(Map.of("a", "b")))).isTrue();
    }

}