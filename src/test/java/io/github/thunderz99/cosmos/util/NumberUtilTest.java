package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class NumberUtilTest {

    @Test
    void convertNumberToIntIfCompatible_should_work() {

        // Test cases where conversion is expected
        assertThat(NumberUtil.convertNumberToIntIfCompatible(123L)).isEqualTo(123); // Long within Integer range
        assertThat(NumberUtil.convertNumberToIntIfCompatible(456)).isEqualTo(456); // Already Integer
        assertThat(NumberUtil.convertNumberToIntIfCompatible(789.0)).isEqualTo(789); // Double with no fractional part
        assertThat(NumberUtil.convertNumberToIntIfCompatible(101.0f)).isEqualTo(101); // Float with no fractional part
        assertThat(NumberUtil.convertNumberToIntIfCompatible(new BigDecimal("101.0"))).isEqualTo(101); // BigDecimal with no fractional part
        assertThat(NumberUtil.convertNumberToIntIfCompatible(new BigDecimal("30"))).isEqualTo(30); // BigDecimal with no fractional part


        // Test cases where original number should be returned
        assertThat(NumberUtil.convertNumberToIntIfCompatible(Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE); // Long out of Integer range
        assertThat(NumberUtil.convertNumberToIntIfCompatible(123.45)).isEqualTo(123.45); // Double with fractional part
        assertThat(NumberUtil.convertNumberToIntIfCompatible(678.9f)).isEqualTo(678.9f); // Float with fractional part
        assertThat(NumberUtil.convertNumberToIntIfCompatible(Integer.MAX_VALUE + 1L)).isEqualTo(Integer.MAX_VALUE + 1L); // Long exactly one more than Integer.MAX_VALUE
        assertThat(NumberUtil.convertNumberToIntIfCompatible(new BigDecimal("35.6494"))).isEqualTo(new BigDecimal("35.6494")); // BigDecimal with fractional part


        // Edge cases
        assertThat(NumberUtil.convertNumberToIntIfCompatible((double) Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE); // Double at the edge of Integer range
        assertThat(NumberUtil.convertNumberToIntIfCompatible((double) Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE); // Double at the edge of Integer range
        assertThat(NumberUtil.convertNumberToIntIfCompatible(null)).isEqualTo(null);
        assertThat(NumberUtil.convertNumberToIntIfCompatible(0)).isEqualTo(0);

    }
}