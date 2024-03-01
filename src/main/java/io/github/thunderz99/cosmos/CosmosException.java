package io.github.thunderz99.cosmos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.microsoft.azure.documentdb.DocumentClientException;

/**
 * Original Exception thrown by java-cosmos. Wrapping v2 DocumentClientException and v4 CosmosException.
 */
public class CosmosException extends RuntimeException {

    static final long serialVersionUID = 1L;

    @JsonIgnore
    private DocumentClientException dce;

    @JsonIgnore
    private com.azure.cosmos.CosmosException ce;

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


    /**
     * Constructor using DocumentClientException;
     *
     * @param dce document client exception
     */
    public CosmosException(DocumentClientException dce) {
        super(dce.getMessage(), dce);
        this.dce = dce;
        this.statusCode = dce.getStatusCode();
        this.code = dce.getError() == null ? "" : dce.getError().getCode();
        this.retryAfterInMilliseconds = dce.getRetryAfterInMilliseconds();
    }

    public CosmosException(com.azure.cosmos.CosmosException ce) {
        super(ce.getMessage(), ce);
        this.ce = ce;
        this.statusCode = ce.getStatusCode();
        // can not get CosmosException's code yet.
        this.code = "";
        this.retryAfterInMilliseconds = ce.getRetryAfterDuration().toMillis();
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
     * @return
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


}
