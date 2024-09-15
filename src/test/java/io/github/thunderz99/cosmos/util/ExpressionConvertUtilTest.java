package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ExpressionConvertUtilTest {

    @Test
    void condition_with_arrayLength_should_work() {
        var expression = "c.age / 10 < ARRAY_LENGTH(c.skills)";
        Map<String, Object> expected =
                Map.of( "$expr",
                    Map.of("$lt",
                        List.of(
                            Map.of("$divide", List.of("$age", 10)),
                            Map.of("$size", Map.of("$ifNull", List.of("$skills", List.of())))
                        )
                    )
                );
        var result = ExpressionConvertUtil.convert(expression);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void min_add_should_work() {
        var expression = "MIN(c['term-test']['scores']) + 5 > 10";
        Map<String, Object> expected =
                Map.of("$expr",
                        Map.of("$gt",
                                List.of(
                                        Map.of("$add",
                                                List.of(Map.of("$min", "$term-test.scores"), 5))
                                        , 10)
                        )
                );
        var result = ExpressionConvertUtil.convert(expression);
        assertThat(result).isEqualTo(expected);
    }


}