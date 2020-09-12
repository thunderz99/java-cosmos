package io.github.thunderz99.cosmos;

import java.util.Map;

import org.json.JSONObject;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * Represent a CosmosDB document. Has a JSONObject inside.
 *
 * Having toObject and toJson util method to convert to Class or String
 * conveniently.
 *
 */
public class CosmosDocument {

	JSONObject jsonObj;

	public CosmosDocument(JSONObject jsonObj) {
		this.jsonObj = jsonObj;
	}

	public <T> T toObject(Class<T> classOfT) {
		return JsonUtil.fromJson(jsonObj.toString(), classOfT);
	}

	public String toJson() {
		return jsonObj.toString();
	}

	public Map<String, Object> toMap() {

		return JsonUtil.toMap(jsonObj.toString());
	}
}