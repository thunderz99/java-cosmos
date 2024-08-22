package io.github.thunderz99.cosmos.dto;

import java.util.List;

/**
 * DTO class representing a uniqueKey in json path. e.g. ["/address/city", "/_uniqueKey1"].
 */
public class UniqueKey extends RecordData {
    public List<String> paths;

    public UniqueKey(){
    }

    public UniqueKey(List<String> paths){
        this.paths = paths;
    }

    /**
     * Getter for paths
     * @return paths
     */
    public List<String> getPaths() {
        return paths;
    }
}
