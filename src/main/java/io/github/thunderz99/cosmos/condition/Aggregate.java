package io.github.thunderz99.cosmos.condition;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Simple data class for aggregate. (e.g. query function, group by)
 */
public class Aggregate {

    /**
     * Default constructor
     */
    public Aggregate() {
    }

    public String function = "";

    public Set<String> groupBy = new LinkedHashSet<>();


    /**
     * condition for the result of aggregation
     */
    public Condition condAfterAggregate = null;

    /**
     * set function
     *
     * <p>
     * aggregate functions like: COUNT, AVG, SUM, MAX, MIN <br>
     * see <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-aggregate-functions">cosmosdb aggregate functions</a>
     * </p>
     *
     * @param function aggregate functions like:
     * @return Aggregate
     */
    public static Aggregate function(String function) {

        Aggregate ret = new Aggregate();
        ret.function = function;

        return ret;
    }


    public Aggregate groupBy(String... fields) {

        if (fields == null || fields.length == 0) {
            return this;
        }

        this.groupBy = new LinkedHashSet<>(List.of(fields));
        return this;
    }

    /**
     * Add filter / sort / limit that applies to the result of aggregation
     *
     * @param condAfterAggregate
     * @return aggregate obj self
     */
    public Aggregate conditionAfterAggregate(Condition condAfterAggregate) {
        this.condAfterAggregate = condAfterAggregate;
        return this;
    }


}
