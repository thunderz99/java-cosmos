package io.github.thunderz99.cosmos.dto;

import io.github.thunderz99.cosmos.util.JsonUtil;

/**
 * A simple bean class with built-in hashCode/equals/toString implement
 */
public abstract class RecordData {

	public RecordData() {
	}

	@Override
	public String toString() {
		return JsonUtil.toJson(this);
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj2) {
		if (obj2 == null) {
			return false;
		}

		return this.toString().equals(obj2.toString());
	}

	/**
	 * deep copy using json util
	 * @param <T> generic param for bean
	 *
	 * @return object copied
	 */
	public <T> T copy() {
		return JsonUtil.fromJson(JsonUtil.toJson(this), this.getClass().getName());
	}
}
