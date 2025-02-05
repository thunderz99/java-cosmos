package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Sets;
import io.github.thunderz99.cosmos.condition.Condition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PGSelectUtilTest {


    @Test
    void generateSelect_should_work() {
        {
            // select with no field
            var cond = Condition.filter()
                    .fields();
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo("*");
        }

        {
            // select with null fields
            var cond = Condition.filter()
                    .fields(null);
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo("*");
        }
    }



    @Test
    void generateSelectByFields_should_work(){
        {
            // normal case 1
            var fields = Sets.newLinkedHashSet(List.of("id", "name"));
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'name', data->'name') AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

        {
            // normal case : complicated keys
            var fields = Sets.newLinkedHashSet(List.of("contents.sheet-1.name", "contents.sheet-2.address"));
            var expected = """
                    id,
                    jsonb_build_object('contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address'))) AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

        {
            // normal case: select with fields that overlaps(sheet-1 overlaps, and sheet-2 standalone)
            var cond = Condition.filter()
                    .fields("id", "contents.sheet-1.name", "contents.sheet-1.age", "contents.sheet-2.address");
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name', 'age', data->'contents'->'sheet-1'->'age'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address'))) AS "data"
                    """;
            assertThat(PGSelectUtil.generateSelect(cond)).isEqualTo(expected);
        }

    }
    
}