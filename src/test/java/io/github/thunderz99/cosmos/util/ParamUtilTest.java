package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class ParamUtilTest {

    @Test
    void get_param_name_should_work() {

        // normal
        assertThat(ParamUtil.getParamNameFromKey("name", 0)).isEqualTo("@param000_name");
        assertThat(ParamUtil.getParamNameFromKey("fullName.last", 1)).isEqualTo("@param001_fullName__last");
        assertThat(ParamUtil.getParamNameFromKey("829cc727-2d49-4d60-8f91-b30f50560af7.name", 1)).matches("@param001_[\\d\\w]{7}__name");
        assertThat(ParamUtil.getParamNameFromKey("family.テスト.age", 2)).matches("@param002_family__[\\d\\w]{7}__age");
        assertThat(ParamUtil.getParamNameFromKey("aa-bb", 2)).matches("@param002_[\\d\\w]{7}");

        // abnormal
        assertThatThrownBy(() -> {
            ParamUtil.getParamNameFromKey("", 1);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key").hasMessageContaining("should be non-blank");

        assertThatThrownBy(() -> {
            ParamUtil.getParamNameFromKey(null, 2);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key").hasMessageContaining("should be non-blank");

        assertThat(ParamUtil.getParamNameFromKey("name", -1)).isEqualTo("@param-01_name");
    }
}