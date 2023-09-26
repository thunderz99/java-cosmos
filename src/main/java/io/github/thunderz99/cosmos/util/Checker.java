package io.github.thunderz99.cosmos.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;

public class Checker {

	Checker(){
	}

	public static void check(boolean result, String message) {

		if (!result) {
			throw new IllegalArgumentException(message);
		}

	}

	public static void checkNotNull(Object target, String name) {

		if (target == null) {
			throw new IllegalArgumentException(String.format("%s should not be null", name));
		}

	}

	public static void checkNotBlank(String target, String name) {

		if (StringUtils.isBlank(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-blank", name));
		}

	}

	public static void checkNotEmpty(String target, String name) {

		if (StringUtils.isEmpty(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-empty", name));
		}

	}

    public static void checkNotEmpty(Collection target, String name) {

        if (target.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s should not be empty collection", name));
        }

    }
}
