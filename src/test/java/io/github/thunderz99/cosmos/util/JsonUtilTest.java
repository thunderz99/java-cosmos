package io.github.thunderz99.cosmos.util;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

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
	public void shouldBeConvertToJsonByClassName2List() {

		var json = "[{\"id\":3}, {\"id\":5}]";

		List<Data> data = JsonUtil.fromJson2List(json, this.getClass().getName() + "$Data");
		assertThat(data.get(1).id).isEqualTo(5);

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
}