package io.github.thunderz99.cosmos.util;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionStringUtil {

    private static Logger log = LoggerFactory.getLogger(ConnectionStringUtil.class);

    static Pattern connectionStringPattern = Pattern.compile("AccountEndpoint=(?<endpoint>.+);AccountKey=(?<key>[^;]+);?");

    public static Pair<String, String> parseConnectionString(String connectionString) {

        var matcher = connectionStringPattern.matcher(connectionString);
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "Make sure connectionString contains 'AccountEndpoint=' and 'AccountKey=' ");
        }
        String endpoint = matcher.group("endpoint");
        String key = matcher.group("key");

        Checker.check(StringUtils.isNotBlank(endpoint), "Make sure connectionString contains 'AccountEndpoint=' ");
        Checker.check(StringUtils.isNotBlank(key), "Make sure connectionString contains 'AccountKey='");

        if (log.isInfoEnabled()) {
            log.info("endpoint:{}", endpoint);
            log.info("key:{}...", key.substring(0, 3));
        }

        return Pair.of(endpoint, key);
    }
}
