package io.github.thunderz99.cosmos;

import com.microsoft.azure.documentdb.DocumentClientException;

/**
 * Original Exception thrown by java-cosmos. Wrapping v2 DocumentClientException and v4 CosmosException.
 */
public class CosmosException extends RuntimeException {

    static final long serialVersionUID = 1L;

    private DocumentClientException dce;

    public CosmosException(DocumentClientException dce) {
        super(dce.getMessage(), dce);
        this.dce = dce;
    }

    /**
     * Get the exception's status code. e.g. 404 / 429 / 403
     * @return status code of exception.
     */
    public int getStatusCode(){
        return dce.getStatusCode();
    }

    /**
     * Get the time to retry suggested by the 429 exception in milliseconds. Return 0 if not 429 exception.
     *
     * @return time to retry in milliseconds
     */
    public long getRetryAfterInMilliseconds(){
        return dce.getRetryAfterInMilliseconds();
    }


}
