package io.github.thunderz99.cosmos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;

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

        if (me.getMessage().contains("DuplicateKey")) {
            return 409;
        }

        return (me instanceof MongoCommandException || me instanceof MongoClientException) ? 400 : 500;
    }
    
}
