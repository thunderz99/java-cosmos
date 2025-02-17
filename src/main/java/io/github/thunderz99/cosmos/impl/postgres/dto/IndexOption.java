package io.github.thunderz99.cosmos.impl.postgres.dto;

/**
 * Options for index. currently only unique or not is supported
 */
public class IndexOption {
    public boolean unique = false;

    /**
     * build an IndexOption with uniqueness specified
     * @param unique
     * @return indexOption
     */
    public static IndexOption unique(boolean unique) {
        var option = new IndexOption();
        option.unique = unique;
        return option;
    }
}
