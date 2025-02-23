package io.github.thunderz99.cosmos.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

/**
 * A util for string hashing
 */
public class HashUtil {


    private static final HashFunction murmurHash3_128 = Hashing.murmur3_128();

    /**
     * Convert string to a shorter MurmurHash3 (128-bit, truncated to 16 hex chars)
     * @param origin
     * @return 16-character MurmurHash3 hash
     */
    public static String toShortHash(String origin) {
        if(origin == null){
            return null;
        }
        return toMurmurHash(origin).substring(0, 16); // Take first 16 chars
    }

    /**
     * Convert string to a full MurmurHash3 (128-bit, 32 hex chars)
     *
     * <p>
     *     We use MurmurHash3 instead of SHA256 because it is shorter and faster. We do not need a SHA256 hash for its security.
     * </p>
     * @param origin
     * @return 32-character MurmurHash3 hash
     */
    public static String toMurmurHash(String origin) {
        if(origin == null){
            return null;
        }
        return murmurHash3_128.hashString(origin, StandardCharsets.UTF_8).toString();
    }

}
