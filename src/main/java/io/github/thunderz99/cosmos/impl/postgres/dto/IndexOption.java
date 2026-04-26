package io.github.thunderz99.cosmos.impl.postgres.dto;

/**
 * Options for index.
 */
public class IndexOption {
    public enum IndexMethod {
        BTREE,
        GIN
    }

    public boolean unique = false;

    /**
     * PostgreSQL index method. default is BTREE.
     */
    public IndexMethod indexMethod = IndexMethod.BTREE;

    /**
     * fieldType for this index. default is text. other valid value is bigint / numeric / float8 / etc
     *
     * <p>
     *     CREATE INDEX idx_expireat_bigint
     *     ON "Schema"."Table" (((data->>'_expireAt')::bigint));
     * </p>
     */
    public String fieldType = "text";

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

    /**
     * set the field type. default is "text"
     * @param fieldType
     * @return indexOption
     */
    public IndexOption fieldType(String fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    /**
     * set the index method. default is BTREE
     * @param indexMethod
     * @return indexOption
     */
    public IndexOption indexMethod(IndexMethod indexMethod) {
        this.indexMethod = indexMethod;
        return this;
    }

    /**
     * use GIN as the index method
     * @return indexOption
     */
    public IndexOption gin() {
        this.indexMethod = IndexMethod.GIN;
        return this;
    }

}
