package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Represent a list of CosmosDB document.
 *
 * <p>
 * Having toList and toJson util method to convert to {@code List<T>} or String
 * conveniently.
 * </p>
 */
public class CosmosDocumentList {

	List<JSONObject> jsonObjs;

	public CosmosDocumentList(List<JSONObject> jsonObjs) {
		this.jsonObjs = jsonObjs;
	}

	public <T> List<T> toList(Class<T> classOfT) {

		if (jsonObjs == null) {
			return List.of();
		}

		return jsonObjs.stream().map(obj -> JsonUtil.fromJson(obj.toString(), classOfT)).collect(Collectors.toList());
	}

	public List<Map<String, Object>> toMap() {
		if (jsonObjs == null) {
			return List.of();
		}
		return jsonObjs.stream().map(obj -> JsonUtil.toMap(obj.toString())).collect(Collectors.toList());
	}

	public int size() {
		return jsonObjs.size();
	}

	public String toJson() {
		return JsonUtil.toJson(jsonObjs);
	}

	public String toString() {
		return toJson();
	}

}
