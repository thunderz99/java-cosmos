package io.github.thunderz99.cosmos.dto;

public class CosmosContainerResponse extends RecordData {

    public String name;
    UniqueKeyPolicy uniqueKeyPolicy = new UniqueKeyPolicy();


    public CosmosContainerResponse() {
    }

    public CosmosContainerResponse(String name) {
        this.name = name;
    }
    
    public CosmosContainerResponse(String name, UniqueKeyPolicy policy) {
        this.name = name;
        this.uniqueKeyPolicy = policy;
    }

    /**
     * Get the uniqueKeyPolicy associated with this container / collection
     *
     * @return uniqueKeyPolicy
     */
    public UniqueKeyPolicy getUniqueKeyPolicy() {
        return uniqueKeyPolicy;
    }
}
