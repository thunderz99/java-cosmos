package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class CronUtilTest {

    @Test
    void isValidPgCronExpression_should_work() {
        {
            // valid cron expression
            assertThat(CronUtil.isValidPgCronExpression("*/1 * * * *")).isTrue();      // Valid: every minute
            assertThat(CronUtil.isValidPgCronExpression("0-30 * * * *")).isTrue();     // Valid: minutes 0-30 every hour
            assertThat(CronUtil.isValidPgCronExpression("0,15,30 * * * *")).isTrue();  // Valid: minutes 0, 15, 30 every hour
        }

        {
            // invalid cron expression
            assertThat(CronUtil.isValidPgCronExpression("60 * * * *")).isFalse();       // Invalid: minute 60 is out of range
            assertThat(CronUtil.isValidPgCronExpression("*/1 * * *")).isFalse();         // Invalid: only 4 fields
            assertThat(CronUtil.isValidPgCronExpression("0 0 12 * * ?")).isFalse();      // Invalid: six fields is not supported
            assertThat(CronUtil.isValidPgCronExpression("0 0 0 * * ?")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("59 59 23 * * ?")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 15 10 * * ?")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 0 12 1/1 * ?")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 0/30 8-17 ? * *")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 0 12 ? * WED")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 11 11 11 11 ?")).isFalse();
            assertThat(CronUtil.isValidPgCronExpression("0 0 0 1,15 * ?")).isFalse();
        }

    }
}