package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CosmosDocumentListTest {

    public static class User{
        public String id = "";
        public String name = "";
    }

    @Test
    void toList_should_work() {
        {
            // Constructor without params. the result list can be modified(not immutable)
            var list = new CosmosDocumentList().toList(User.class);
            list.add(new User());
            assertThat(list).hasSize(1);
            assertThat(list.get(0).name).isEqualTo("");
        }

        {
            // Constructor with empty list. the result list can be modified(not immutable)
            var list = new CosmosDocumentList(List.of()).toList(User.class);
            list.add(new User());
            assertThat(list).hasSize(1);
            assertThat(list.get(0).name).isEqualTo("");
        }

        {
            // Constructor with list. the result list can be modified(not immutable)
            var list = new CosmosDocumentList(Lists.newArrayList(Map.of("id", "ID001", "name", "Tom"))).toList(User.class);
            list.add(new User());
            assertThat(list).hasSize(2);
            assertThat(list.get(0).name).isEqualTo("Tom");
        }

    }

    @Test
    void toMap_should_work() {
        {
            // Constructor without params. the result list can be modified(not immutable)
            var mapList = new CosmosDocumentList().toMap();
            mapList.add(Map.of("a", 1));
            assertThat(mapList).hasSize(1);
            assertThat(mapList.get(0).get("a")).isEqualTo(1);
        }
        {
            // Constructor without params. the result list can be modified(not immutable)
            var mapList = new CosmosDocumentList(List.of()).toMap();
            mapList.add(Map.of("a", 1));
            assertThat(mapList).hasSize(1);
            assertThat(mapList.get(0).get("a")).isEqualTo(1);
        }

        {
            // Constructor with list. the result list can be modified(not immutable)
            var mapList = new CosmosDocumentList(Lists.newArrayList(Map.of("id", "ID001", "name", "Tom"))).toMap();
            mapList.add(Map.of("a", 1));
            assertThat(mapList).hasSize(2);
            assertThat(mapList.get(0).get("name")).isEqualTo("Tom");
        }

    }
}