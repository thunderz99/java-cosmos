package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Sets;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.SubConditionType;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class PGConditionUtilTest {

    static final String coll = "schema1";
    static final String partition = "table1";

    @Test
    void toQuerySpec_should_work() {


        {
            // test with no fields and no sort
            var condNoFields = Condition.filter();
            var actualSpecNoFields = PGConditionUtil.toQuerySpec(coll, condNoFields, partition);
            var expectedNoFields = new CosmosSqlQuerySpec("SELECT *\n FROM schema1.table1\n OFFSET 0 LIMIT 100", new ArrayList<>());
            assertThat(actualSpecNoFields).isEqualTo(expectedNoFields);
        }

        {
            // test with specific filters
            {
                // test with specific filters
                var condFilter = Condition.filter().filter("age", 18);
                var actualSpecFilter = PGConditionUtil.toQuerySpec(coll, condFilter, partition);
                var expectedFilter = new CosmosSqlQuerySpec("SELECT *\n FROM schema1.table1\n WHERE ((data->>'age')::numeric = @param000_age) OFFSET 0 LIMIT 100", List.of(new CosmosSqlParameter("@param000_age", 18)));
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
                    \n FROM coll.partition
                    \n ORDER BY data->>'name' COLLATE "C" ASC, data->>'_ts' ASC OFFSET 0 LIMIT 100
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

            var queryTextExpected = " WHERE (data->>'id' = @param000_id) AND ((data->>'age')::numeric > @param001_age)";
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
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'fullName'->>'last' = @param000_fullName__last) AND ((data->>'position' = @param001_position) OR (data->>'organization' = @param002_organization)) AND ((data->>'age')::numeric = @param003_age)
                     ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20
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
            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'position' = @param000_position)) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText()).isEqualTo(expected.trim());
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
            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'position' = @param000_position)) AND ((data->>'address' = @param001_address)) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText()).isEqualTo(expected.trim());
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

        var expected = """
                SELECT *
                 FROM schema1.table1
                 WHERE ((data->>'isChecked')::boolean = @param000_isChecked) AND ((data->>'id_A' = @param001_id_A) OR (data->>'id_B' = @param002_id_B) AND (data->>'id_C' = @param003_id_C)) OFFSET 0 LIMIT 100
                """;

        assertThat(q.getQueryText()).isEqualTo(expected.trim());
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
        var expected = """
                SELECT *
                 FROM schema1.table1
                 WHERE ((data->>'position' = @param000_position) OR (data->>'organization' = @param001_organization))
                 ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20
                """;
        assertThat(q.getQueryText().trim()).isEqualTo(
                expected.trim());

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

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'position' = @param000_position) AND (data->>'organization' = @param001_organization))
                     ORDER BY data->>'_ts' DESC OFFSET 10 LIMIT 20
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(
                    expected.trim());

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
            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'position' = @param000_position)) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText()).isEqualTo(expected.trim());
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
            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'position' = @param000_position)) AND (1=1) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText()).isEqualTo(expected.trim());
            var params = List.copyOf(q.getParameters());
            assertThat(params).hasSize(1);
            assertThat(params.get(0).toJson()).isEqualTo(new CosmosSqlParameter("@param000_position", "leader").toJson());
        }
    }

    @Test
    void buildQuerySpec_should_work_for_empty() {

        var cond = Condition.filter();
        var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

        var expected = """
                SELECT *
                 FROM schema1.table1
                 OFFSET 0 LIMIT 100
                """;
        assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());

        assertThat(q.getParameters()).isEmpty();

    }

    @Test
    void buildQuerySpec_should_work_for_empty_sub_query() {

        {
            var cond = Condition.filter(SubConditionType.OR, //
                    List.of());
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).isEmpty();
        }

        {
            var cond = Condition.filter(SubConditionType.OR, //
                    List.of(Condition.filter("id", 1)));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (((data->>'id')::numeric = @param000_id)) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim())
                    .isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.AND, //
                    List.of());
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'id')::numeric = @param000_id) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim())
                    .isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter(SubConditionType.AND, //
                    List.of(Condition.filter(), Condition.filter()));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(0);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter()));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'id')::numeric = @param000_id) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim())
                    .isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(1);
        }

        {
            var cond = Condition.filter("id", 1, SubConditionType.OR, //
                    List.of(Condition.filter(), Condition.filter("name", "Tom")));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE ((data->>'id')::numeric = @param000_id) AND ((data->>'name' = @param001_name)) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(
                    expected.trim());
            assertThat(q.getParameters()).hasSize(2);
        }

    }

    @Test
    void processNegativeQuery_should_work() {
        assertThat(PGConditionUtil.processNegativeQuery(null, true)).isNull();
        assertThat(PGConditionUtil.processNegativeQuery(null, false)).isNull();
        assertThat(PGConditionUtil.processNegativeQuery("", true)).isEmpty();
        assertThat(PGConditionUtil.processNegativeQuery("", false)).isEmpty();

        assertThat(PGConditionUtil.processNegativeQuery("(data->>'age')::numeric >= 1", false)).isEqualTo("(data->>'age')::numeric >= 1");
        assertThat(PGConditionUtil.processNegativeQuery("(data->>'age')::numeric >= 1", true)).isEqualTo(" NOT((data->>'age')::numeric >= 1)");

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
            assertThat(q).isEqualTo(" ORDER BY data->>'name' COLLATE \"C\" ASC, data->>'_ts' ASC");
        }

        {
            var sorts = List.of("name", "ASC", "age", "DESC");
            var q = PGConditionUtil.buildSorts(sorts);
            var expected = """
                    ORDER BY data->>'name' COLLATE "C" ASC,\s
                      CASE jsonb_typeof(data->'age')
                        WHEN 'null' THEN 0
                        WHEN 'boolean' THEN 1
                        WHEN 'number' THEN 2
                        WHEN 'string' THEN 3
                        WHEN 'array' THEN 4
                        WHEN 'object' THEN 5
                        ELSE 6
                      END DESC,
                      CASE
                        WHEN jsonb_typeof(data->'age') = 'string'
                          THEN data->>'age' COLLATE "C"
                        ELSE NULL
                      END DESC,
                      data->'age' DESC
                    , data->>'_ts' ASC
                    """;
            assertThat(q.trim()).isEqualTo(expected.trim());
        }

        {
            var sorts = List.of("id", "DESC", "_ts", "ASC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY data->>'id' COLLATE \"C\" DESC, data->>'_ts' ASC");
        }

        {
            // build sort should work for type specify(text)
            var sorts = List.of("employCode::text", "DESC", "_ts", "ASC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY data->>'employCode' COLLATE \"C\" DESC, data->>'_ts' ASC");
        }

        {
            // build sort should work for type specify(int)
            var sorts = List.of("age::int", "ASC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY (data->>'age')::int ASC, data->>'_ts' ASC");
        }

        {
            // build sort should work for type specify(numeric)
            var sorts = List.of("sort::numeric", "DESC");
            var q = PGConditionUtil.buildSorts(sorts);
            assertThat(q).isEqualTo(" ORDER BY (data->>'sort')::numeric DESC, data->>'_ts' DESC");
        }

        {
            // build sort should work for type specify(wrong type)
            var sorts = List.of("sort::double", "DESC");
            var q = PGConditionUtil.buildSorts(sorts);
            var expected = """
                     ORDER BY\s
                      CASE jsonb_typeof(data->'sort')
                        WHEN 'null' THEN 0
                        WHEN 'boolean' THEN 1
                        WHEN 'number' THEN 2
                        WHEN 'string' THEN 3
                        WHEN 'array' THEN 4
                        WHEN 'object' THEN 5
                        ELSE 6
                      END DESC,
                      CASE
                        WHEN jsonb_typeof(data->'sort') = 'string'
                          THEN data->>'sort' COLLATE "C"
                        ELSE NULL
                      END DESC,
                      data->'sort' DESC
                    , data->>'_ts' DESC
                    """;
            assertThat(q.trim()).isEqualTo(expected.trim());
        }


    }


    @Test
    void buildQuerySpec_should_work_for_simple_join() {
        // query with join
        {

            {
                // returnAllSubArray false
                var cond = Condition.filter("area.city.street.rooms.no", "001", "room*no-01.area", 10) //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("area.city.street.rooms", "room*no-01"))
                        .returnAllSubArray(false);

                var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

                var actual = q.getQueryText().replaceAll("@param[0-9a-zA-Z_]+", "@PARAM");
                var expected = """
                        SELECT id, jsonb_set(
                          jsonb_set(
                          data,
                          '{"area","city","street","rooms"}',
                          COALESCE(
                             (
                           SELECT jsonb_agg(s2)
                           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS s2
                           WHERE ( (s2->>'no' = @PARAM))
                         ),
                            data->'area'->'city'->'street'->'rooms'
                          )
                        )
                        ,
                          '{"room*no-01"}',
                          COALESCE(
                             (
                           SELECT jsonb_agg(s3)
                           FROM jsonb_array_elements(data->'room*no-01') AS s3
                           WHERE ( ((s3->>'area')::numeric = @PARAM))
                         ),
                            data->'room*no-01'
                          )
                        )
                         AS data
                         FROM schema1.table1
                         WHERE EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS j0
                           WHERE (j0->>'no' = @PARAM)
                         ) AND EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(data->'room*no-01') AS j1
                           WHERE ((j1->>'area')::numeric = @PARAM)
                         )
                         ORDER BY data->>'id' COLLATE "C" ASC, data->>'_ts' ASC OFFSET 0 LIMIT 10
                        """;

                assertThat(actual).isEqualTo(expected.trim());

                assertThat(q.getParameters()).hasSize(4);

            }

            {
                // returnAllSubArray true
                var cond = Condition.filter("area.city.street.rooms.no", "001") //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("area.city.street.rooms"))
                        .returnAllSubArray(true);

                var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

                var expected = """
                        SELECT *
                         FROM schema1.table1
                         WHERE EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS j0
                           WHERE (j0->>'no' = @param000_no)
                         )
                         ORDER BY data->>'id' COLLATE "C" ASC, data->>'_ts' ASC OFFSET 0 LIMIT 10
                        """;
                assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());

                assertThat(q.getParameters()).hasSize(1);


            }

            {
                // LIKE
                var cond = Condition.filter("area.city.street.rooms.no LIKE", "%01") //
                        .sort("id", "ASC") //
                        .limit(10) //
                        .offset(0)
                        .join(Set.of("area.city.street.rooms"))
                        .returnAllSubArray(false);


                var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

                var expected = """
                        SELECT id, jsonb_set(
                          data,
                          '{"area","city","street","rooms"}',
                          COALESCE(
                             (
                           SELECT jsonb_agg(s1)
                           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS s1
                           WHERE ( (s1->>'no' LIKE @param001_no__for_select))
                         ),
                            data->'area'->'city'->'street'->'rooms'
                          )
                        )
                         AS data
                         FROM schema1.table1
                         WHERE EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS j0
                           WHERE (j0->>'no' LIKE @param000_no)
                         )
                         ORDER BY data->>'id' COLLATE "C" ASC, data->>'_ts' ASC OFFSET 0 LIMIT 10
                        """;
                assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());

                assertThat(q.getParameters()).hasSize(2);

            }
        }
    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_any() {

        // without cond.join
        {
            // normal case
            var cond = Condition.filter("rooms ARRAY_CONTAINS_ANY", List.of("003", "009"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'rooms' @> @param000_rooms__0::jsonb OR data->'rooms' @> @param000_rooms__1::jsonb) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("\"003\"");

        }

        {
            // normal case for integer
            var cond = Condition.filter("rooms ARRAY_CONTAINS_ANY", List.of(3, 9));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'rooms' @> @param000_rooms__0::jsonb OR data->'rooms' @> @param000_rooms__1::jsonb) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("3");

        }


        {
            // with filter key "no"
            var cond = Condition.filter("rooms ARRAY_CONTAINS_ANY no",  List.of("003", "009"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param000_rooms__no__0)) OR data->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param000_rooms__no__1))) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__no__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("003");

        }

    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_all() {

        // without cond.join
        {
            // normal case
            var cond = Condition.filter("rooms ARRAY_CONTAINS_ALL", List.of("003", "009"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'rooms' @> @param000_rooms__0::jsonb AND data->'rooms' @> @param000_rooms__1::jsonb) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("\"009\"");

        }

        {
            // with filter key "no"
            var cond = Condition.filter("rooms ARRAY_CONTAINS_ALL no",  List.of("003", "009"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE (data->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param000_rooms__no__0)) AND data->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param000_rooms__no__1))) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__no__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("009");

        }

    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_any_with_join_using_both_name_and_no() {

        {
            // with returnAllSubArray=false
            // and filters contain both "name" and "no" on the same join key "floors.rooms"
            // see docs/postgres-find-with-join.md sample SQL 2
            var cond = Condition.filter(
                    "floors.rooms ARRAY_CONTAINS_ANY name",  List.of("r1", "r2"),
                            "floors.rooms ARRAY_CONTAINS_ANY no",  List.of("001", "002")
                    )
                    .join(Set.of("floors"))
                    .returnAllSubArray(false)
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT id, jsonb_set(
                      data,
                      '{"floors"}',
                      COALESCE(
                         (
                       SELECT jsonb_agg(s2)
                       FROM jsonb_array_elements(data->'floors') AS s2
                       WHERE ( (s2->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param002_rooms__name__0__for_select)) OR s2->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param002_rooms__name__1__for_select)))
                       AND (s2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param003_rooms__no__0__for_select)) OR s2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param003_rooms__no__1__for_select))))
                     ),
                        data->'floors'
                      )
                    )
                     AS data
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     ) AND EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j1
                       WHERE (j1->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param001_rooms__no__0)) OR j1->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param001_rooms__no__1)))
                     ) OFFSET 0 LIMIT 100
                    """;

            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(8);

            // for "name" contains ["r1", "r2"]
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__name__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r2");

            // for "no" contains ["001", "002"]
            assertThat(q.getParameters().get(2).getName()).isEqualTo("@param001_rooms__no__0");
            assertThat(q.getParameters().get(2).getValue()).isEqualTo("001");
            assertThat(q.getParameters().get(3).getName()).isEqualTo("@param001_rooms__no__1");
            assertThat(q.getParameters().get(3).getValue()).isEqualTo("002");

            // for "name" contains ["r1", "r2"] in SELECT
            assertThat(q.getParameters().get(4).getName()).isEqualTo("@param002_rooms__name__0__for_select");
            assertThat(q.getParameters().get(4).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(5).getName()).isEqualTo("@param002_rooms__name__1__for_select");
            assertThat(q.getParameters().get(5).getValue()).isEqualTo("r2");

            // for "no" contains ["001", "002"] in SELECT
            assertThat(q.getParameters().get(6).getName()).isEqualTo("@param003_rooms__no__0__for_select");
            assertThat(q.getParameters().get(6).getValue()).isEqualTo("001");
            assertThat(q.getParameters().get(7).getName()).isEqualTo("@param003_rooms__no__1__for_select");
            assertThat(q.getParameters().get(7).getValue()).isEqualTo("002");

        }

    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_all_with_join_using_both_name_and_no() {

        {
            // with returnAllSubArray=false
            // and filters contain both "name" and "no" on the same join key "floors.rooms"
            // see docs/postgres-find-with-join.md sample SQL 2
            var cond = Condition.filter(
                            "floors.rooms ARRAY_CONTAINS_ALL name",  List.of("r1", "r2"),
                            "floors.rooms ARRAY_CONTAINS_ALL no",  List.of("001", "002")
                    )
                    .join(Set.of("floors"))
                    .returnAllSubArray(false)
                    .fields("id", "floors", "_ts")
                    .sort("id", "ASC")
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    WITH filtered_data AS (
                    SELECT id, jsonb_set(
                      data,
                      '{"floors"}',
                      COALESCE(
                         (
                       SELECT jsonb_agg(s2)
                       FROM jsonb_array_elements(data->'floors') AS s2
                       WHERE ( (s2->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param002_rooms__name__0__for_select)) AND s2->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param002_rooms__name__1__for_select)))
                       AND (s2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param003_rooms__no__0__for_select)) AND s2->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param003_rooms__no__1__for_select))))
                     ),
                        data->'floors'
                      )
                    )
                     AS data
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) AND j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     ) AND EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j1
                       WHERE (j1->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param001_rooms__no__0)) AND j1->'rooms' @> jsonb_build_array(jsonb_build_object('no', @param001_rooms__no__1)))
                     )
                    )
                    SELECT
                    id,
                    jsonb_build_object('id', data->'id', 'floors', data->'floors', '_ts', data->'_ts') AS "data"
                    
                    FROM filtered_data
                     ORDER BY data->>'id' COLLATE "C" ASC, data->>'_ts' ASC OFFSET 0 LIMIT 100
                    """;

            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(8);

            // for "name" contains ["r1", "r2"]
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__name__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r2");

            // for "no" contains ["001", "002"]
            assertThat(q.getParameters().get(2).getName()).isEqualTo("@param001_rooms__no__0");
            assertThat(q.getParameters().get(2).getValue()).isEqualTo("001");
            assertThat(q.getParameters().get(3).getName()).isEqualTo("@param001_rooms__no__1");
            assertThat(q.getParameters().get(3).getValue()).isEqualTo("002");

            // for "name" contains ["r1", "r2"] in SELECT
            assertThat(q.getParameters().get(4).getName()).isEqualTo("@param002_rooms__name__0__for_select");
            assertThat(q.getParameters().get(4).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(5).getName()).isEqualTo("@param002_rooms__name__1__for_select");
            assertThat(q.getParameters().get(5).getValue()).isEqualTo("r2");

            // for "no" contains ["001", "002"] in SELECT
            assertThat(q.getParameters().get(6).getName()).isEqualTo("@param003_rooms__no__0__for_select");
            assertThat(q.getParameters().get(6).getValue()).isEqualTo("001");
            assertThat(q.getParameters().get(7).getName()).isEqualTo("@param003_rooms__no__1__for_select");
            assertThat(q.getParameters().get(7).getValue()).isEqualTo("002");

        }

    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_any_with_join() {

        // with cond.join
        {
            // normal case
            var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ANY", List.of("003", "009")).join(Set.of("rooms"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'rooms') AS j0
                       WHERE (j0->'no' @> @param000_no__0::jsonb OR j0->'no' @> @param000_no__1::jsonb)
                     ) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_no__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("\"003\"");

        }

        {
            // with filter key "no"
            var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ANY name",  List.of("r1", "r2")).join(Set.of("floors"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     ) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");

        }

        {
            // with returnAllSubArray=false
            var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ANY name",  List.of("r1", "r2"))
                    .join(Set.of("floors"))
                    .returnAllSubArray(false)
                    .offset(0).limit(10)
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT id, jsonb_set(
                      data,
                      '{"floors"}',
                      COALESCE(
                         (
                       SELECT jsonb_agg(s1)
                       FROM jsonb_array_elements(data->'floors') AS s1
                       WHERE ( (s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__0__for_select)) OR s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__1__for_select))))
                     ),
                        data->'floors'
                      )
                    )
                     AS data
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     ) OFFSET 0 LIMIT 10
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(4);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__name__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r2");

            assertThat(q.getParameters().get(2).getName()).isEqualTo("@param001_rooms__name__0__for_select");
            assertThat(q.getParameters().get(2).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(3).getName()).isEqualTo("@param001_rooms__name__1__for_select");
            assertThat(q.getParameters().get(3).getValue()).isEqualTo("r2");

        }

        {
            // with returnAllSubArray=false and fields, sort
            var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ANY name",  List.of("r1", "r2"))
                    .join(Set.of("floors"))
                    .returnAllSubArray(false)
                    .fields("id", "address.street", "area*no-1")
                    .sort("_ts", "DESC", "address.street", "ASC")
                    .offset(0).limit(10)
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    WITH filtered_data AS (
                    SELECT id, jsonb_set(
                      data,
                      '{"floors"}',
                      COALESCE(
                         (
                       SELECT jsonb_agg(s1)
                       FROM jsonb_array_elements(data->'floors') AS s1
                       WHERE ( (s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__0__for_select)) OR s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__1__for_select))))
                     ),
                        data->'floors'
                      )
                    )
                     AS data
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     )
                    )
                    SELECT
                    id,
                    jsonb_build_object('id', data->'id', 'address', jsonb_build_object('street', data->'address'->'street'), 'area*no-1', data->'area*no-1') AS "data"
                    
                    FROM filtered_data
                     ORDER BY data->>'_ts' DESC,\s
                      CASE jsonb_typeof(data->'address'->'street')
                        WHEN 'null' THEN 0
                        WHEN 'boolean' THEN 1
                        WHEN 'number' THEN 2
                        WHEN 'string' THEN 3
                        WHEN 'array' THEN 4
                        WHEN 'object' THEN 5
                        ELSE 6
                      END ASC,
                      CASE
                        WHEN jsonb_typeof(data->'address'->'street') = 'string'
                          THEN data->'address'->>'street' COLLATE "C"
                        ELSE NULL
                      END ASC,
                      data->'address'->'street' ASC
                     OFFSET 0 LIMIT 10
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(4);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__name__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r2");

            assertThat(q.getParameters().get(2).getName()).isEqualTo("@param001_rooms__name__0__for_select");
            assertThat(q.getParameters().get(2).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(3).getName()).isEqualTo("@param001_rooms__name__1__for_select");
            assertThat(q.getParameters().get(3).getValue()).isEqualTo("r2");

        }

    }

    @Test
    void buildQuerySpec_should_work_for_array_contains_all_with_join() {

        // with cond.join
        {
            // normal case
            var cond = Condition.filter("rooms.no ARRAY_CONTAINS_ALL", List.of("003", "009")).join(Set.of("rooms"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'rooms') AS j0
                       WHERE (j0->'no' @> @param000_no__0::jsonb AND j0->'no' @> @param000_no__1::jsonb)
                     ) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_no__0");

        }

        {
            // with filter key "no"
            var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ALL name",  List.of("r1", "r2")).join(Set.of("floors"));
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT *
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) AND j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
                     ) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param000_rooms__name__1");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r2");

        }

        {
            // with returnAllSubArray=false
            var cond = Condition.filter("floors.rooms ARRAY_CONTAINS_ALL name",  List.of("r1"))
                    .join(Set.of("floors"))
                    .returnAllSubArray(false)
                    ;
            var q = PGConditionUtil.toQuerySpec(coll, cond, partition);

            var expected = """
                    SELECT id, jsonb_set(
                      data,
                      '{"floors"}',
                      COALESCE(
                         (
                       SELECT jsonb_agg(s1)
                       FROM jsonb_array_elements(data->'floors') AS s1
                       WHERE ( (s1->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param001_rooms__name__0__for_select))))
                     ),
                        data->'floors'
                      )
                    )
                     AS data
                     FROM schema1.table1
                     WHERE EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(data->'floors') AS j0
                       WHERE (j0->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)))
                     ) OFFSET 0 LIMIT 100
                    """;
            assertThat(q.getQueryText().trim()).isEqualTo(expected.trim());
            assertThat(q.getParameters()).hasSize(2);
            assertThat(q.getParameters().get(0).getName()).isEqualTo("@param000_rooms__name__0");
            assertThat(q.getParameters().get(0).getValue()).isEqualTo("r1");
            assertThat(q.getParameters().get(1).getName()).isEqualTo("@param001_rooms__name__0__for_select");
            assertThat(q.getParameters().get(1).getValue()).isEqualTo("r1");

        }


    }

}