package io.github.thunderz99.cosmos.util;

import java.security.InvalidParameterException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonUtilTest {

    public static class Data {
        @JsonProperty
        public int id;

        public Data() {
        }

        public Data(int id) {
            this.id = id;
        }
    }

    public static class DataWithEnum {
        @JsonProperty
        public int id;

        public Type secondType;
        public Type firstType;
        public Type type;


        public void setFirstType(Type firstType) {
            this.firstType = firstType;
            this.secondType = firstType;
        }
    }

    public enum Type {
        STRING, INTEGER, OBJECT
    }

    @Test
    public void shouldBeConvertToJson() {

        var data = new Data(1);
        assertThat(JsonUtil.toJson(data)).isEqualTo(String.format("{%n  \"id\" : 1%n}"));
    }

    @Test
    public void shouldBeConvertToJsonByClassName() {

        var json = "{\"id\":3}";

        Data data = JsonUtil.fromJson(json, this.getClass().getName() + "$Data");
        assertThat(data.id).isEqualTo(3);

    }

    @Test
    public void shouldConvertToList() {

        var json = "[{\"id\":3}, {\"id\":5}]";

        List<Data> data = JsonUtil.fromJson2List(json, this.getClass().getName() + "$Data");
        assertThat(data.get(1).id).isEqualTo(5);

    }

    @Test
    public void shouldConvertToListOfMap() {

        var json = "[{\"id\":3}, {\"id\":5}]";

        var data = JsonUtil.toListOfMap(json);
        assertThat(data.get(1).get("id")).isEqualTo(5);

    }

    @Test
    public void shouldBeConvertToNoIndentJson() {
        var data = new Data(1);
        assertThat(JsonUtil.toJsonNoIndent(data)).isEqualTo("{\"id\":1}");
    }

    @Test
    public void shouldBeConvertFromJson() {
        var json = "{\"id\":1}";
        assertThat(JsonUtil.fromJson(json, Data.class).id).isEqualTo(1);
    }

    @Test
    void shouldBeError() {
        var json = "{\"id\":1, \"name\":}";
        assertThatThrownBy(() -> JsonUtil.fromJson(json, Data.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("json process error.");
    }

    @Test
    void shouldBeEnumNullWithoutError() {
        var json = "{\"id\":1,\"secondType\":\"INTEGER\",\"firstType\":\"STRING\",\"type\":\"STRING1\"}";
        DataWithEnum dataWithEnum = JsonUtil.fromJson(json, DataWithEnum.class);
        assertThat(dataWithEnum).isNotNull();
        assertThat(dataWithEnum.id).isEqualTo(1);
        assertThat(dataWithEnum.type).isEqualTo(null);
        assertThat(dataWithEnum.firstType).isEqualTo(Type.STRING);
        assertThat(dataWithEnum.secondType).isEqualTo(Type.STRING);
    }

    @Test
    void exceptionToStringShouldBeOk() {
        Throwable ex = new InvalidParameterException("invalid parameter");
        assertThat(JsonUtil.exceptionToString(ex)).startsWith("java.security.InvalidParameterException: invalid parameter\n" +
                "\tat io.github.thunderz99.cosmos.util.JsonUtilTest.exceptionToStringShouldBeOk(JsonUtilTest.java:115)");
    }
}