package io.github.thunderz99.cosmos.condition;

import com.microsoft.azure.documentdb.SqlParameterCollection;
import io.github.thunderz99.cosmos.dto.RecordData;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple bean class represents the query text and parameters for Filter
 * part.(e.g WHERE xxx)
 */
public class FilterQuery extends RecordData {


	/**
	 * queryText parts
	 */
	public StringBuilder queryText;

	/**
	 * params parts
	 */
	public SqlParameterCollection params;

	/**
	 * condition index for global query. Always increment for a new filter cond.
	 *
	 * <p>
	 * The first one should be WHERE and followed by AND ...
	 * </p>
	 */
	public AtomicInteger conditionIndex;

	/**
	 * param index for global query. Always increment for a new param.
	 *
	 * <p>
	 * The first one should be "@param000_foo" and followed by "@param001_bar",
	 * "@param002_baz" ...
	 * </p>
	 */
	public AtomicInteger paramIndex;

	public FilterQuery() {
	}

	public FilterQuery(String queryText, SqlParameterCollection params, AtomicInteger conditionIndex,
					   AtomicInteger paramIndex) {
		this.queryText = new StringBuilder(queryText);
		this.params = params;
		this.conditionIndex = conditionIndex;
		this.paramIndex = paramIndex;
	}

}
