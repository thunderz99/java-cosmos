package io.github.thunderz99.cosmos;

import com.microsoft.azure.documentdb.DocumentClientException;

public class CosmosException extends RuntimeException {

    static final long serialVersionUID = 1L;

    private DocumentClientException dce;
    private com.azure.cosmos.CosmosException ce;

    public CosmosException(DocumentClientException dce) {
        super(dce.getMessage(), dce);
        this.dce = dce;
    }

    public CosmosException(com.azure.cosmos.CosmosException ce) {
        super(ce.getMessage(), ce);
        this.ce = ce;
    }

    /**
     * Get the exception's status code. e.g. 404 / 429 / 403
     * @return status code of exception.
     */
    public int getStatusCode(){
        return dce == null ? ce.getStatusCode() : dce.getStatusCode();
    }

    /**
     * Get the time to retry suggested by the 429 exception in milliseconds. Return 0 if not 429 exception.
     *
     * @return time to retry in milliseconds
     */
    public long getRetryAfterInMilliseconds(){
        return dce == null ? ce.getRetryAfterDuration().toMillis() : dce.getRetryAfterInMilliseconds();
    }


}
