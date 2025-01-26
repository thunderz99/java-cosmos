package io.github.thunderz99.cosmos.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;

/**
 * A util class to check if some conditions are met. If not, throw IllegalArgumentException
 *
 */
public class Checker {

	Checker(){
	}

	/**
	 * Check if the result is true. If not, throw IllegalArgumentException with message
	 * @param result
	 * @param message
	 */
	public static void check(boolean result, String message) {

		if (!result) {
			throw new IllegalArgumentException(message);
		}

	}

	/**
	 * Check if the target is not null. If null, throw IllegalArgumentException with name
	 * @param target
	 * @param name
	 * @return target
	 */
	public static Object checkNotNull(Object target, String name) {

		if (target == null) {
			throw new IllegalArgumentException(String.format("%s should not be null", name));
		}

        return target;

	}

	/**
	 * Check if the target is not blank. If blank, throw IllegalArgumentException with name
	 * @param target
	 * @param name
	 * @return target
	 */
	public static String checkNotBlank(String target, String name) {

		if (StringUtils.isBlank(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-blank", name));
		}

        return target;

	}

	/**
	 * Check if the target is not empty. If empty, throw IllegalArgumentException with name
	 * @param target
	 * @param name
	 * @return target
	 */
	public static String checkNotEmpty(String target, String name) {

		if (StringUtils.isEmpty(target)) {
			throw new IllegalArgumentException(String.format("%s should be non-empty", name));
		}

        return target;

	}

    /**
     * Check if the target is not empty collection. If empty, throw IllegalArgumentException with name
     * @param target
     * @param name
     * @return target
     */
    public static <T> Collection<T> checkNotEmpty(Collection<T> target, String name) {

        if (target == null || target.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s should not be empty collection", name));
        }

        return target;
    }

    /**
     * Check if the target is not empty array. If empty, throw IllegalArgumentException with name
     * @param target
     * @param name
     * @return target
     */
    public static <T> T[] checkNotEmpty(T[] target, String name) {

        if (target == null || target.length == 0) {
            throw new IllegalArgumentException(String.format("%s should not be empty array", name));
        }

        return target;
    }
}
