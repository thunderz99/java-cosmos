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

	public static Object checkNotNull(Object target, String name) {

		if (target == null) {
			throw new IllegalArgumentException(String.format("%s should not be null", name));
		}

        return target;

	}

	public static String checkNotBlank(String target, String name) {

		if (StringUtils.isBlank(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-blank", name));
		}

        return target;

	}

	public static String checkNotEmpty(String target, String name) {

		if (StringUtils.isEmpty(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-empty", name));
		}

        return target;

	}

    public static <T> Collection<T> checkNotEmpty(Collection<T> target, String name) {

        if (target == null || target.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s should not be empty collection", name));
        }

        return target;
    }
}
