package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Sets;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class PGConditionUtilTest {

    static final String coll = "schema1";
    static final String partition = "table1";


    @Test
    void generateSelect_should_work() {
        {
            // select with no field
            var cond = Condition.filter()
                    .fields();
            assertThat(PGConditionUtil.generateSelect(cond)).isEqualTo("*");
        }

        {
            // select with null fields
            var cond = Condition.filter()
                    .fields(null);
            assertThat(PGConditionUtil.generateSelect(cond)).isEqualTo("*");
        }

        {
            // select with 2 fields
            var cond = Condition.filter()
                    .fields("id", "name");
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'name', data->'name') AS "data"
                    """;
            assertThat(PGConditionUtil.generateSelect(cond)).isEqualTo(expected);
        }
        {
            // select with fields that overlaps(sheet-1 overlaps, and sheet-2 standalone)
            var cond = Condition.filter()
                    .fields("id", "contents.sheet-1.name", "contents.sheet-1.age", "contents.sheet-2.address");
            var expected = """
                    id,
                    jsonb_build_object('id', data->'id', 'contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name', 'age', data->'contents'->'sheet-1'->'age'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address'))) AS "data"
                    """;
            assertThat(PGConditionUtil.generateSelect(cond)).isEqualTo(expected);
        }
    }

    @Test
    void toQuerySpec_should_work() {


        {
            // test with no fields and no sort
            var condNoFields = Condition.filter();
            var actualSpecNoFields = PGConditionUtil.toQuerySpec(coll, condNoFields, partition);
            var expectedNoFields = new CosmosSqlQuerySpec("SELECT * FROM schema1.table1 OFFSET 0 LIMIT 100", new ArrayList<>());
            assertThat(actualSpecNoFields).isEqualTo(expectedNoFields);
        }

        {
            // test with specific filters
            {
                // test with specific filters
                var condFilter = Condition.filter().filter("age", 18);
                var actualSpecFilter = PGConditionUtil.toQuerySpec(coll, condFilter, partition);
                var expectedFilter = new CosmosSqlQuerySpec("SELECT * FROM schema1.table1 WHERE ((data->>'age')::int = @param000_age) OFFSET 0 LIMIT 100", List.of(new CosmosSqlParameter("@param000_age", 18)));
                assertThat(actualSpecFilter).isEqualTo(expectedFilter);
            }
        }

        {
            // test with specific fields and sort
            var condFieldsSort = Condition.filter().fields("id", "name").sort("name", "ASC");
            var actualSpecFieldsSort = PGConditionUtil.toQuerySpec("coll", condFieldsSort, "partition");

            var expectedSQL= """
                    SELECT id,
                    jsonb_build_object('id', data->'id', 'name', data->'name') AS \"data\"
                     FROM coll.partition ORDER BY data->>'name' ASC, data->>'_ts' ASC OFFSET 0 LIMIT 100
                    """;
            var expectedFieldsSort = new CosmosSqlQuerySpec(expectedSQL.trim(), new ArrayList<>());
            assertThat(actualSpecFieldsSort).isEqualTo(expectedFieldsSort);
        }

        {
            // test with rawQuerySpec
            var rawQuerySpec = new CosmosSqlQuerySpec("SELECT * FROM data");
            var condRaw = new Condition();
            condRaw.rawQuerySpec = rawQuerySpec;
            var actualSpecRaw = PGConditionUtil.toQuerySpec("coll", condRaw, "partition");
            assertThat(actualSpecRaw).isEqualTo(rawQuerySpec);
        }
    }


    @Test
    void generateFilterQuery_should_work() {
        {
            // normal case with 2 filter
            var cond = Condition.filter("id", "001", "age >", 15);
            var filterQuery = PGConditionUtil.generateFilterQuery(cond, "", new ArrayList<>(), new AtomicInteger(), new AtomicInteger(), QueryContext.create());

            var queryTextExpected = " WHERE (data->>'id' = @param000_id) AND ((data->>'age')::int > @param001_age)";
            assertThat(filterQuery.queryText.toString()).isEqualTo(queryTextExpected);
            assertThat(filterQuery.params).hasSize(2);
            assertThat(filterQuery.params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_id", "001").toJson());
            assertThat(filterQuery.params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_age", 15).toJson());
        }

        {
            // nested fields
            var cond = Condition.filter("address.city <=", "NY", "expired", true);
            var filterQuery = PGConditionUtil.generateFilterQuery(cond, "", new ArrayList<>(), new AtomicInteger(), new AtomicInteger(), QueryContext.create());

            var queryTextExpected = " WHERE (data->'address'->>'city' <= @param000_address__city) AND ((data->>'expired')::boolean = @param001_expired)";
            assertThat(filterQuery.queryText.toString()).isEqualTo(queryTextExpected);
            assertThat(filterQuery.params).hasSize(2);
            assertThat(filterQuery.params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_address__city", "NY").toJson());
            assertThat(filterQuery.params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_expired", true).toJson());
        }

    }

    @Test
    void buildQuerySpec_should_work_for_sub_cond_or() {
        {
            var cond = Condition.filter("fullName.last", "Hanks", //
                            SubConditionType.OR,
                            List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")), //
                            "age", 30) //
                    .sort("_ts", "DESC") //
                    .offset(10) //
                    .limit(20) //
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expectedSQL = """
                    SELECT * FROM schema1.table1 WHERE (data->'fullName'->>'last' = @param000_fullName__last) AND ((data->>'position' = @param001_position) OR (data->>'organization' = @param002_organization)) AND ((data->>'age')::int = @param003_age) ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expectedSQL.trim());

            var params = List.copyOf(q.getParameters());

            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_fullName__last", "Hanks").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_position", "leader").toJson());
            assertThat(params.get(2).toJson()).isEqualTo(new CosmosSqlParameter("@param002_organization", "executive").toJson());
            assertThat(params.get(3).toJson()).isEqualTo(new CosmosSqlParameter("@param003_age", 30).toJson());
        }

        {
            //sub query in single condition without a List
            var cond = Condition.filter(SubConditionType.OR,
                            Condition.filter("position", "leader"))
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }

        {
            //multiple SUB_COND_OR s
            var cond = Condition.filter(
                            SubConditionType.OR,
                            Condition.filter("position", "leader"),
                            SubConditionType.OR + " 2",
                            Condition.filter("address", "London")
                    );
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position)) AND ((data->>'address' = @param001_address)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_address", "London").toJson());
        }
    }


    @Test
    void buildQuerySpec_should_work_for_and_nested_with_or() {

        var cond = Condition.filter("isChecked", true,
                        "$OR or_first ",
                        List.of(Condition.filter("id_A", "value_A"),
                                Condition.filter("id_B", "value_B", "id_C", "value_C")));
        var q = PGConditionUtil.toQuerySpec(coll, cond, partition);


        assertThat(q.getQueryText()).isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'isChecked')::boolean = @param000_isChecked) AND ((data->>'id_A' = @param001_id_A) OR (data->>'id_B' = @param002_id_B) AND (data->>'id_C' = @param003_id_C)) OFFSET 0 LIMIT 100");
    }

    @Test
    void buildQuerySpec_should_work_for_sub_cond_or_from_the_beginning() {

        var cond = Condition.filter( //
                        SubConditionType.OR,
                        List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
                ) //
                .sort("_ts", "DESC") //
                .offset(10) //
                .limit(20);
        var q = PGConditionUtil.toQuerySpec(coll, cond, partition);
        assertThat(q.getQueryText().trim()).isEqualTo(
                "SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position) OR (data->>'organization' = @param001_organization)) ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20");

        var params = List.copyOf(q.getParameters());

        assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_organization", "executive").toJson());
    }


    @Test
    void buildQuerySpec_should_work_for_sub_cond_and() {

        {
            // SUB_COND_AND mixed with sort, offset, limit
            var cond = Condition.filter( //
                            SubConditionType.AND,
                            List.of(Condition.filter("position", "leader"), Condition.filter("organization", "executive")) //
                    ) //
                    .sort("_ts", "DESC") //
                    .offset(10) //
                    .limit(20) //
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position) AND (data->>'organization' = @param001_organization)) ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20");

            var params = List.copyOf(q.getParameters());

            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
            assertThat(params.get(1).toJson()).isEqualTo(new CosmosSqlParameter("@param001_organization", "executive").toJson());
        }

        {
            //sub query in single condition without a List
            var cond = Condition.filter(SubConditionType.AND,
                            Condition.filter("position", "leader"))
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position)) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }

        {
            //multiple SUB_COND_AND s
            var cond = Condition.filter(
                            SubConditionType.AND,
                            Condition.filter("position", "leader"),
                            SubConditionType.AND + " another",
                            List.of(Condition.rawSql("1=1"))
                    )
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);
            assertThat(q.getQueryText()).isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'position' = @param000_position)) AND (1=1) OFFSET 0 LIMIT 100");
            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }
    }

    @Test
    void buildQuerySpec_should_work_for_empty() {

        var cond = Condition.filter();
        var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

        assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM schema1.table1 OFFSET 0 LIMIT 100");

        assertThat(q.getParameters()).isEmpty();

    }

    @Test
    void buildQuerySpec_should_work_for_empty_sub_query() {

        {
            var cond = Condition.filter(SubConditionType.OR, //
                    List.of());
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM schema1.table1 OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).isEmpty();
        }

        {
            var cond = Condition.filter(SubConditionType.OR, //
                    List.of(Condition.filter("id", 1)));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM schema1.table1 WHERE (((data->>'id')::int = @param000_id)) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.AND, //
                    List.of());
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'id')::int = @param000_id) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter(SubConditionType.AND, //
                    List.of(Condition.filter(), Condition.filter()));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim()).isEqualTo("SELECT * FROM schema1.table1 OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(0);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter()));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim())
                    .isEqualTo("SELECT * FROM schema1.table1 WHERE ((data->>'id')::int = @param000_id) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter(), Condition.filter("name", "Tom")));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            assertThat(q.getQueryText().trim()).isEqualTo(
                    "SELECT * FROM schema1.table1 WHERE ((data->>'id')::int = @param000_id) AND ((data->>'name' = @param001_name)) OFFSET 0 LIMIT 100");
            assertThat(q.getParameters()).hasSize(2);
        }

    }

    @Test
    void processNegativeQuery_should_work() {
        assertThat(PGConditionUtil.processNegativeQuery(null, true)).isNull();
        assertThat(PGConditionUtil.processNegativeQuery(null, false)).isNull();
        assertThat(PGConditionUtil.processNegativeQuery("", true)).isEmpty();
        assertThat(PGConditionUtil.processNegativeQuery("", false)).isEmpty();

        assertThat(PGConditionUtil.processNegativeQuery("(data->>'age')::int >= 1", false)).isEqualTo("(data->>'age')::int >= 1");
        assertThat(PGConditionUtil.processNegativeQuery("(data->>'age')::int >= 1", true)).isEqualTo(" NOT((data->>'age')::int >= 1)");

    }

    @Test
    void buildSorts_should_work() {
        {
            var sorts = new ArrayList<String>();
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEmpty();
        }
        {
            var sorts = List.of("name", "ASC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY data->>'name' ASC, data->>'_ts' ASC");
        }

        {
            var sorts = List.of("name", "ASC", "age", "DESC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY data->>'name' ASC, data->>'age' DESC, data->>'_ts' ASC");
        }

        {
            var sorts = List.of("id", "DESC", "_ts", "ASC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY data->>'id' DESC, data->>'_ts' ASC");
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
            assertThat(PGConditionUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

        {
            // normal case 2
            var fields = Sets.newLinkedHashSet(List.of("contents.sheet-1.name", "contents.sheet-2.address"));
            var expected = """
                    id,
                    jsonb_build_object('contents', jsonb_build_object('sheet-1', jsonb_build_object('name', data->'contents'->'sheet-1'->'name'), 'sheet-2', jsonb_build_object('address', data->'contents'->'sheet-2'->'address')), 'id', data->'id') AS "data"
                    """;
            assertThat(PGConditionUtil.generateSelectByFields(fields)).isEqualTo(expected);
        }

    }


}