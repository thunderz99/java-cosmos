package io.github.thunderz99.cosmos;

/**
 * Thrown before database access when a compiled query exceeds a library safety limit.
 */
public class QueryTooComplexException extends IllegalArgumentException {

    public QueryTooComplexException(String message) {
        super(message);
    }
}
