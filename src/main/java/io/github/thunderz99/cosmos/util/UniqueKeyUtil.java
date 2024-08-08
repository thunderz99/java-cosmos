package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.dto.UniqueKey;
import io.github.thunderz99.cosmos.dto.UniqueKeyPolicy;
import org.apache.commons.collections4.CollectionUtils;

/**
 * An util class to help to convert uniqueKey policy and do other util functions.
 */
public class UniqueKeyUtil {

    /**
     * Convert java cosmos dto policy to azure cosmos dto
     * @param policy
     * @return
     */
    public static com.azure.cosmos.models.UniqueKeyPolicy toCosmosUniqueKeyPolicy(UniqueKeyPolicy policy){
        if(policy == null){
            return null;
        }

        if(CollectionUtils.isEmpty(policy.uniqueKeys)){
            return new com.azure.cosmos.models.UniqueKeyPolicy();
        }

        var ret = new com.azure.cosmos.models.UniqueKeyPolicy();
        ret.setUniqueKeys(policy.uniqueKeys.stream().map( k -> new com.azure.cosmos.models.UniqueKey(k.paths)).collect(Collectors.toList()));
        return ret;
    }


    /**
     * return the unique key policy by key
     *
     * <p>
     * key starts with "/".  e.g.  "/users/title"
     * </p>
     *
     * @param keys fields to generate uniqueKeyPolicy
     * @return unique key policy
     */
    public static UniqueKeyPolicy getUniqueKeyPolicy(Set<String> keys) {
        var uniqueKeyPolicy = new UniqueKeyPolicy();

        if (CollectionUtils.isEmpty(keys)) {
            return uniqueKeyPolicy;
        }

        uniqueKeyPolicy.uniqueKeys = keys.stream().map(key -> toUniqueKey(key)).collect(Collectors.toList());

        return uniqueKeyPolicy;
    }

    /**
     * generate a uniqueKey obj from a string
     *
     * @param key
     * @return
     */
    static UniqueKey toUniqueKey(String key) {
        Checker.checkNotBlank(key, "uniqueKey");
        var ret = new UniqueKey(List.of(key));
        return ret;
    }

    /**
     * Convert azure cosmos policy to java-cosmos policy
     * @param policy
     */
    public static UniqueKeyPolicy toCommonUniqueKeyPolicy(com.azure.cosmos.models.UniqueKeyPolicy policy) {
        if(policy == null){
            return null;
        }

        if(CollectionUtils.isEmpty(policy.getUniqueKeys())){
            return new UniqueKeyPolicy();
        }

        var ret = new UniqueKeyPolicy();

        ret.uniqueKeys = policy.getUniqueKeys().stream().map(k -> new UniqueKey(k.getPaths())).collect(Collectors.toList());

        return ret;

    }
}
