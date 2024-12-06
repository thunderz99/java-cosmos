package io.github.thunderz99.cosmos.util;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
/**
 * A util class to generate param names from a given key.
 */
public class ParamUtil {

    public static final Pattern alphaNumericPattern = Pattern.compile("^[\\w\\d]+$");

    /**
     * generate a valid param name from a key.
     * <p>
     * {@code
     * // e.g
     * // fullName.last -> @param001_fullName__last
     * // or
     * // if the key contains a non alphanumeric part, it will be replaced by a random str.
     * // "829cc727-2d49-4d60-8f91-b30f50560af7.name" -> @param001_wg31gsa.name
     * // "family.テスト.age" -> @param001_family.ab135dx.age
     * }
     *
     * @param key
     * @param index
     */
    public static String getParamNameFromKey(String key, int index) {
        Checker.checkNotBlank(key, "key");

        var sanitizedKey = Stream.of(key.split("\\.")).map(part -> {
            if (alphaNumericPattern.asMatchPredicate().test(part)) {
                return part;
            } else {
                return RandomStringUtils.randomAlphanumeric(7);
            }
        }).collect(Collectors.joining("__"));

        return String.format("@param%03d_%s", index, sanitizedKey);
    }
}
