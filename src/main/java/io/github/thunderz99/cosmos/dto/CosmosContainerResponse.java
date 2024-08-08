package io.github.thunderz99.cosmos.dto;

public class CosmosContainerResponse extends RecordData {
    UniqueKeyPolicy uniqueKeyPolicy = new UniqueKeyPolicy();


    public CosmosContainerResponse(){}

    public CosmosContainerResponse(UniqueKeyPolicy policy){
        this.uniqueKeyPolicy = policy;
    }

    /**
     * Get the uniqueKeyPolicy associated with this container / collection
     * @return uniqueKeyPolicy
     */
    public UniqueKeyPolicy getUniqueKeyPolicy() {
        return uniqueKeyPolicy;
    }
}
