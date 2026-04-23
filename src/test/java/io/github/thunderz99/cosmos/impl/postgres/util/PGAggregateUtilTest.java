package io.github.thunderz99.cosmos.impl.postgres.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PGAggregateUtilTest {

    @Test
    void getSimpleName_should_work(){
        assertThat(PGAggregateUtil.getSimpleName(null)).isEqualTo(null);
        assertThat(PGAggregateUtil.getSimpleName("")).isEqualTo("");
        assertThat(PGAggregateUtil.getSimpleName("room.area")).isEqualTo("area");
        assertThat(PGAggregateUtil.getSimpleName("fullName.last")).isEqualTo("last");
        assertThat(PGAggregateUtil.getSimpleName("age")).isEqualTo("age");
        assertThat(PGAggregateUtil.getSimpleName("1")).isEqualTo("1");
    }

    @Test
    void convertAggregateResultsToInteger_should_work() {
        // Setup

        // Test data setup
        List<Map<String, Object>> testMaps = new ArrayList<>();
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
        var resultMaps = PGAggregateUtil.convertAggregateResultsToInteger(testMaps);

        // Assertions
        assertThat(resultMaps).isNotNull();
        assertThat(resultMaps.size()).isEqualTo(3);

        assertThat(resultMaps.get(0).get("itemsCount")).isInstanceOf(Integer.class).isEqualTo(1);
        assertThat(resultMaps.get(1).get("itemsCount")).isInstanceOf(Long.class).isEqualTo(Long.MAX_VALUE); // Should remain Long because it's out of Integer range
        assertThat(resultMaps.get(2).get("itemsCount")).isInstanceOf(Integer.class).isEqualTo(100);
        assertThat(resultMaps.get(2).get("itemsWithinRange")).isInstanceOf(Integer.class).isEqualTo(Integer.MAX_VALUE); // Should remain Integer
    }

    @Test
    void convertAggregateResultsToInteger_should_preserve_numeric_strings() {
        List<Map<String, Object>> testMaps = new ArrayList<>();
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("employeeCode", "123");
        map.put("facetCount", 2L);
        testMaps.add(map);

        var resultMaps = PGAggregateUtil.convertAggregateResultsToInteger(testMaps);

        assertThat(resultMaps).hasSize(1);
        assertThat(resultMaps.get(0).get("employeeCode")).isInstanceOf(String.class).isEqualTo("123");
        assertThat(resultMaps.get(0).get("facetCount")).isInstanceOf(Integer.class).isEqualTo(2);
    }
}
