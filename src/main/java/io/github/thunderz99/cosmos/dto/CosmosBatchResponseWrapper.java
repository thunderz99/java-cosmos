package io.github.thunderz99.cosmos.dto;

import java.time.Duration;

import com.azure.cosmos.models.CosmosBatchResponse;

/**
 * A dto class representing the batch result of CosmosDB.
 *
 * <p>
 *     We use this wrapper object to overcame the problem that we cannot new an original CosmosBatchReponse to test
 * </p>
 */
public class CosmosBatchResponseWrapper {

    public CosmosBatchResponseWrapper(){
    }

    public CosmosBatchResponseWrapper(CosmosBatchResponse response){
        this.cosmosBatchReponse = response;
    }

    public CosmosBatchResponseWrapper(int statusCode, int subStatusCode, String errorMessage){
        this.statusCode = statusCode;
        this.subStatusCode = subStatusCode;
        this.errorMessage = errorMessage;
    }

    public CosmosBatchResponseWrapper(int statusCode, int subStatusCode, String errorMessage, Duration duration){
        this.statusCode = statusCode;
        this.subStatusCode = subStatusCode;
        this.errorMessage = errorMessage;
        this.duration = duration;
    }


    public CosmosBatchResponse cosmosBatchReponse;

    public int statusCode = 0;

    public int subStatusCode = 0;

    public String errorMessage = "";

    public Duration duration = Duration.ZERO;

    /**
     * Get the statusCode from cosmosBatchReponse
     * @return statusCode
     */
    public int getStatusCode() {
        if(cosmosBatchReponse != null){
            return cosmosBatchReponse.getStatusCode();
        }
        return statusCode;
    }

    /**
     * Get the subStatusCode from cosmosBatchReponse
     * @return subStatusCode
     */
    public int getSubStatusCode() {
        if(cosmosBatchReponse != null){
            return cosmosBatchReponse.getSubStatusCode();
        }
        return subStatusCode;
    }

    /**
     * Get the subStatusCode from cosmosBatchReponse
     * @return subStatusCode
     */
    public String getErrorMessage() {
        if(cosmosBatchReponse != null){
            return cosmosBatchReponse.getErrorMessage();
        }
        return errorMessage;
    }

    /**
     * Get whether statusCode is successful
     * @return isSuccessStatusCode
     */
    public boolean isSuccessStatusCode() {
        if(cosmosBatchReponse != null){
            return cosmosBatchReponse.isSuccessStatusCode();
        }
        return statusCode >= 200 && statusCode < 300;
    }

    /**
     * Get the retryAfterDuration
     * @return retryAfterDuration
     */
    public Duration getRetryAfterDuration() {
        if(cosmosBatchReponse != null){
            return cosmosBatchReponse.getRetryAfterDuration();
        }
        return duration;
    }
}
