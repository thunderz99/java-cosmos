package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.dto.UniqueKeyPolicy;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UniqueKeyUtilTest {


    @Test
    void toCosmosUniqueKeyPolicy_should_work() throws Exception {
        // abnormal cases
        {
            assertThat(UniqueKeyUtil.toCosmosUniqueKeyPolicy(null)).isNull();
            assertThat(UniqueKeyUtil.toCosmosUniqueKeyPolicy(new UniqueKeyPolicy()).getUniqueKeys()).isEmpty();
        }

        // normal cases
        {
            var policy = UniqueKeyUtil.getUniqueKeyPolicy(Sets.newLinkedHashSet("/_uniqueKey1", "/_uniqueKey2"));
            var converted = UniqueKeyUtil.toCosmosUniqueKeyPolicy(policy);
            assertThat(converted.getUniqueKeys()).hasSize(2);
            assertThat(converted.getUniqueKeys().get(0).getPaths()).hasSize(1).containsExactly("/_uniqueKey1");
            assertThat(converted.getUniqueKeys().get(1).getPaths()).hasSize(1).containsExactly("/_uniqueKey2");
        }

    }


    @Test
    void toCommonUniqueKeyPolicy_should_work() throws Exception {
        // abnormal cases
        {
            assertThat(UniqueKeyUtil.toCommonUniqueKeyPolicy(null)).isNull();
            assertThat(UniqueKeyUtil.toCommonUniqueKeyPolicy(new com.azure.cosmos.models.UniqueKeyPolicy()).uniqueKeys).isEmpty();
        }

        // normal cases
        {
            var policy = UniqueKeyUtil.toCosmosUniqueKeyPolicy(UniqueKeyUtil.getUniqueKeyPolicy(Sets.newLinkedHashSet("/_uniqueKey1", "/_uniqueKey2")));
            var converted = UniqueKeyUtil.toCommonUniqueKeyPolicy(policy);
            assertThat(converted.uniqueKeys).hasSize(2);
            assertThat(converted.uniqueKeys.get(0).paths).hasSize(1).containsExactly("/_uniqueKey1");
            assertThat(converted.uniqueKeys.get(1).paths).hasSize(1).containsExactly("/_uniqueKey2");
        }

    }

    @Test
    void getUniqueKeyPolicy_should_work() throws Exception {
        var policy =  UniqueKeyUtil.getUniqueKeyPolicy(Sets.newLinkedHashSet("/_uniqueKey1", "/address/city"));
        assertThat(policy.uniqueKeys).hasSize(2);
        assertThat(policy.uniqueKeys.get(0).paths).hasSize(1).containsExactly("/_uniqueKey1");
        assertThat(policy.uniqueKeys.get(1).paths).hasSize(1).containsExactly("/address/city");
    }

    @Test
    void toUniqueKey_should_work() throws Exception {
        var uniqueKey = UniqueKeyUtil.toUniqueKey("/users/title");
        assertThat(uniqueKey.paths).hasSize(1).containsExactly("/users/title");
    }



}