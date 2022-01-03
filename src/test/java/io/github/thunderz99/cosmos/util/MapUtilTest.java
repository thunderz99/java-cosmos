package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

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


}