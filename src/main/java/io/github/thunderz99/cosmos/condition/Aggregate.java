package io.github.thunderz99.cosmos.condition;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
	 * set function
	 *
	 * <p>
	 *     aggregate functions like: COUNT, AVG, SUM, MAX, MIN <br>
	 *     see <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-aggregate-functions">cosmosdb aggregate functions</a>
	 * </p>
	 *
	 *
	 * @param function aggregate functions like:
	 * @return Aggregate
	 */
	public static Aggregate function(String function) {

		if(StringUtils.isEmpty(function)){
			throw new IllegalArgumentException("function cannot be empty");
		}

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

}
