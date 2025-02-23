package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class HashUtilTest {

    @Test
    void toShortHash_should_work() {
        {
            // normal case
            var origin = "java-cosmos";
            var expected = "3041dc471d6d50a4";
            assertThat(HashUtil.toShortHash(origin)).isEqualTo(expected).hasSize(16);
        }

        {
            // empty string
            var origin = "";
            var expected = "0000000000000000";
            assertThat(HashUtil.toShortHash(origin)).isEqualTo(expected);
        }

        {
            // null
            String origin = null;
            String expected = null;
            assertThat(HashUtil.toShortHash(origin)).isEqualTo(expected);
        }

        {
            // long string
            var origin = IntStream.range(0, 512).mapToObj(i -> "x").collect(Collectors.joining());
            var expected = "5afb5f26f9450479";
            assertThat(HashUtil.toShortHash(origin)).isEqualTo(expected);
        }
    }
}