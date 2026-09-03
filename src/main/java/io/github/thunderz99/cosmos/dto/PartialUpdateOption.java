package io.github.thunderz99.cosmos.dto;

/**
 * Options when using partial update methods. e.g. whether check etags to implement a
 */
public class PartialUpdateOption {

    public boolean checkETag = false;

    /**
     * Whether an explicitly supplied empty nested map should replace the existing map.
     * The default is {@code false} to preserve the legacy deep-merge behavior.
     */
    public boolean replaceEmptyMap = false;

    public static PartialUpdateOption checkETag(boolean checkETag){
        var option = new PartialUpdateOption();
        option.checkETag = checkETag;
        return option;
    }

    /**
     * Configure whether an explicitly supplied empty nested map replaces the existing map.
     *
     * @param replaceEmptyMap {@code true} to replace an existing nested map with an empty map
     * @return this option
     */
    public PartialUpdateOption replaceEmptyMap(boolean replaceEmptyMap) {
        this.replaceEmptyMap = replaceEmptyMap;
        return this;
    }

}
