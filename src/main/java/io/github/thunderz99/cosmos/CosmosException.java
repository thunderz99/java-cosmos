package io.github.thunderz99.cosmos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PSQLException;

/**
 * Original Exception thrown by java-cosmos. Wrapping v2 DocumentClientException and v4 CosmosException.
 */
public class CosmosException extends RuntimeException {

    static final long serialVersionUID = 1L;

    @JsonIgnore
    private Exception e;

    /**
     * http status code
     */
    int statusCode;

    /**
     * String error code. e.g. Unauthorized / NotFound / etc
     */
    String code;

    /**
     * T time to retry suggested by the 429 exception in milliseconds. 0 if not 429 exception.
     */
    long retryAfterInMilliseconds;

    public CosmosException(com.azure.cosmos.CosmosException ce) {
        super(ce.getMessage(), ce);
        this.e = ce;
        this.statusCode = ce.getStatusCode();
        // can not get CosmosException's code yet.
        this.code = "";
        this.retryAfterInMilliseconds = ce.getRetryAfterDuration().toMillis();
    }

    public CosmosException(MongoException me) {
        super(me.getMessage(), me);
        this.e = me;
        this.statusCode = convertStatusCode(me);
        this.code = String.valueOf(me.getCode());
        this.retryAfterInMilliseconds = 0;
    }

    public CosmosException(PSQLException pe) {
        super(pe.getMessage(), pe);
        this.e = pe;
        this.statusCode = convertStatusCode(pe);
        this.code = String.valueOf(pe.getSQLState());
        this.retryAfterInMilliseconds = 0;
    }


    /**
     * Constructor using statusCode and message
     *
     * @param statusCode cosmos exception's httpStatusCode
     * @param code       string error code
     * @param message    detail message
     */
    public CosmosException(int statusCode, String code, String message) {
        super(message);
        this.statusCode = statusCode;
        this.code = code;
    }

    /**
     * Constructor using statusCode and message
     *
     * @param statusCode cosmos exception's httpStatusCode
     * @param code       string error code
     * @param message    detail message
     * @param cause      cause exception
     */
    public CosmosException(int statusCode, String code, String message, Exception cause) {
        super(message);
        this.statusCode = statusCode;
        this.code = code;
        this.e = cause;
    }

    /**
     * Constructor using statusCode and message
     *
     * @param statusCode               cosmos exception's httpStatusCode
     * @param code                     string error code
     * @param message                  detail message
     * @param retryAfterInMilliseconds amount of time should retry after in milliseconds
     */
    public CosmosException(int statusCode, String code, String message, long retryAfterInMilliseconds) {
        super(message);
        this.statusCode = statusCode;
        this.code = code;
        this.retryAfterInMilliseconds = retryAfterInMilliseconds;
    }



    /**
     * Get the exception's status code. e.g. 404 / 429 / 403
     *
     * @return status code of exception.
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Get the string code e.g. Unauthorized / NotFound / etc
     *
     * @return code for exception (http status)
     */
    public String getCode() {
        return code;
    }

    /**
     * Get the time to retry suggested by the 429 exception in milliseconds. Return 0 if not 429 exception.
     *
     * @return time to retry in milliseconds
     */
    public long getRetryAfterInMilliseconds() {
        return retryAfterInMilliseconds;
    }

    /**
     * Convert mongo exception's error code / error message to statusCode for Restful
     *
     * @param me
     * @return
     */
    static int convertStatusCode(MongoException me) {

        var message = me.getMessage();
        if (message.contains("DuplicateKey") || message.contains("duplicate key") || message.contains("E11000")) {
            return 409;
        }

        return (me instanceof MongoCommandException || me instanceof MongoClientException) ? 400 : 500;
    }

    /**
     * Convert postgres exception's error code / error message to statusCode for Restful
     *
     * @param pe
     * @return
     */
    static int convertStatusCode(PSQLException pe) {

        var message = pe.getMessage();

        /**
         * 23505	unique_violation
         * https://www.postgresql.org/docs/current/errcodes-appendix.html
         */
        if ("23505".equals(pe.getSQLState()) || message.contains("duplicate key") || message.contains("DuplicateKey")) {
            return 409;
        }

        if(StringUtils.startsWithAny(pe.getSQLState(), "08000", "08003", "08006", "08001", "08004", "08007")){
            /**
             * should retry
             * Class 08 — Connection Exception
             * https://www.postgresql.org/docs/current/errcodes-appendix.html
             */
            return 429;
        }

        /**
         * Classes that often suggest client-side or application-related issues (but still reported by the server):
         *
         * '22' - Data Exception (Class 22): These errors usually mean the client sent data that the server couldn't process due to data type mismatches, invalid format, etc. The error is reported by the server because it's validating the data, but the cause is often in the client's data or SQL query.
         *
         * '23' - Integrity Constraint Violation (Class 23): These errors (like 23505 - unique violation) occur when the client's operation violates database constraints defined on the server. Again, the server reports the error, but the client's action triggered it.
         *
         * '42' - Syntax Error or Access Rule Violation (Class 42): Errors like 42601 (syntax error) or 42501 (insufficient privilege) mean the client sent invalid SQL or tried to access something it's not allowed to. The server is reporting the error based on the client's request.
         */
        return (StringUtils.startsWithAny(pe.getSQLState(), "22", "23", "42")) ? 400 : 500;
    }

}
