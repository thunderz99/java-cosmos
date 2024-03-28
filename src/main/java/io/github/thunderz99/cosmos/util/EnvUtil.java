package io.github.thunderz99.cosmos.util;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.lang3.StringUtils;

/**
 * A simple util to get env variables from System's Environment variables or .env file
 */
public class EnvUtil {

    static Dotenv dotenv = Dotenv.configure().ignoreIfMalformed().ignoreIfMissing().load();

    /**
     * get env variable as String, or return a default value.
     *
     * @param envName      env variable name
     * @param defaultValue default value when env variable not exist
     * @return return default value of not found
     */
    public static String getOrDefault(String envName, String defaultValue) {
        var value = System.getenv(envName);
        return StringUtils.isNotEmpty(value) ? value : dotenv.get(envName, defaultValue == null ? "" : defaultValue);

    }

    /**
     * get env variable as String
     * @param envName env variable name
     * @return env variable value
     */
    public static String get(String envName) {
        var value = System.getenv(envName);
        if (StringUtils.isEmpty(value)) {
            value = dotenv.get(envName);
        }
        return value;
    }
}
