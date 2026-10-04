package io.github.thunderz99.cosmos.util;

import com.azure.cosmos.implementation.patch.PatchOperationCore;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.slf4j.Logger;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sanitizes JSON-compatible values immediately before they are persisted.
 *
 * <p>The general-purpose {@link JsonUtil} conversions intentionally remain unchanged. This class only removes
 * real U+0000 characters from string values supplied to persistence operations.</p>
 */
public final class PersistenceDataSanitizer {

    private static final char NUL = '\u0000';

    private PersistenceDataSanitizer() {
    }

    /**
     * Convert a document to a map and remove U+0000 from string values at any depth.
     *
     * @param data source document
     * @return sanitized map and removal details
     */
    public static SanitizationResult<Map<String, Object>> sanitizeDocument(Object data) {
        var removals = new ArrayList<Removal>();
        var map = JsonUtil.toMap(data);
        var sanitized = sanitizeMap(map, "", removals);
        return new SanitizationResult<>(sanitized, removals);
    }

    /**
     * Create a sanitized copy of patch operations. The supplied operations are never modified.
     *
     * @param operations source patch operations
     * @return sanitized operations and removal details
     */
    public static SanitizationResult<PatchOperations> sanitizePatchOperations(PatchOperations operations) {
        var removals = new ArrayList<Removal>();
        var sanitized = PatchOperations.create();

        for (var rawOperation : operations.getPatchOperations()) {
            var operation = (PatchOperationCore<?>) rawOperation;
            var type = operation.getOperationType();
            var path = operation.getPath();
            var value = operation.getResource();

            switch (type) {
                case ADD -> sanitized.add(path, sanitizePatchValue(value, path, removals));
                case REMOVE -> sanitized.remove(path);
                case REPLACE -> sanitized.replace(path, sanitizePatchValue(value, path, removals));
                case SET -> sanitized.set(path, sanitizePatchValue(value, path, removals));
                case INCREMENT -> copyIncrement(sanitized, path, value);
                default -> throw new UnsupportedOperationException("Unsupported JSON Patch operation: " + type);
            }
        }

        return new SanitizationResult<>(sanitized, removals);
    }

    /**
     * Log removal metadata without logging the original business value.
     */
    public static void logIfChanged(Logger logger, String operation, String collection, String partition,
                                    String documentId, SanitizationResult<?> result) {
        if (!logger.isInfoEnabled()) {
            return;
        }

        for (var removal : result.removals()) {
            logger.info("Removed U+0000 before persistence. operation:{}, collection:{}, partition:{}, "
                            + "documentId:{}, path:{}, removedCount:{}",
                    operation, collection, partition, documentId, removal.path(), removal.count());
        }
    }

    private static Object sanitizePatchValue(Object value, String path, List<Removal> removals) {
        if (value == null || value instanceof CharSequence || isBinary(value) || isScalar(value)) {
            return sanitizeValue(value, path, removals);
        }

        if (value instanceof Map<?, ?> || value instanceof Collection<?> || value.getClass().isArray()) {
            return sanitizeValue(value, path, removals);
        }

        return sanitizeValue(JsonUtil.toMap(value), path, removals);
    }

    private static Map<String, Object> sanitizeMap(Map<String, Object> map, String path, List<Removal> removals) {
        var sanitized = new LinkedHashMap<String, Object>();
        for (var entry : map.entrySet()) {
            var childPath = appendPath(path, entry.getKey());
            sanitized.put(entry.getKey(), sanitizeValue(entry.getValue(), childPath, removals));
        }
        return sanitized;
    }

    private static Object sanitizeValue(Object value, String path, List<Removal> removals) {
        if (value instanceof String string) {
            return sanitizeString(string, path, removals);
        }

        if (value instanceof CharSequence characters) {
            return sanitizeString(characters.toString(), path, removals);
        }

        if (value instanceof Character character) {
            return sanitizeString(character.toString(), path, removals);
        }

        if (value instanceof Map<?, ?> map) {
            var stringKeyMap = new LinkedHashMap<String, Object>();
            for (var entry : map.entrySet()) {
                var key = String.valueOf(entry.getKey());
                var childPath = appendPath(path, key);
                stringKeyMap.put(key, sanitizeValue(entry.getValue(), childPath, removals));
            }
            return stringKeyMap;
        }

        if (value instanceof Collection<?> collection) {
            var sanitized = new ArrayList<>(collection.size());
            var index = 0;
            for (var element : collection) {
                sanitized.add(sanitizeValue(element, appendPath(path, Integer.toString(index)), removals));
                index++;
            }
            return sanitized;
        }

        if (value != null && value.getClass().isArray() && !isBinary(value)) {
            var sanitized = new ArrayList<>(Array.getLength(value));
            for (var index = 0; index < Array.getLength(value); index++) {
                sanitized.add(sanitizeValue(Array.get(value, index), appendPath(path, Integer.toString(index)), removals));
            }
            return sanitized;
        }

        return value;
    }

    private static boolean isBinary(Object value) {
        return value instanceof byte[] || value instanceof ByteBuffer;
    }

    private static boolean isScalar(Object value) {
        return value instanceof Number
                || value instanceof Boolean
                || value instanceof Character
                || value instanceof Enum<?>
                || value instanceof Date
                || value instanceof Calendar
                || value instanceof TemporalAccessor
                || value instanceof UUID;
    }

    private static String sanitizeString(String value, String path, List<Removal> removals) {
        var firstNul = value.indexOf(NUL);
        if (firstNul < 0) {
            return value;
        }

        var builder = new StringBuilder(value.length());
        builder.append(value, 0, firstNul);
        var count = 0;
        for (var index = firstNul; index < value.length(); index++) {
            var character = value.charAt(index);
            if (character == NUL) {
                count++;
            } else {
                builder.append(character);
            }
        }
        removals.add(new Removal(path.isEmpty() ? "/" : path, count));
        return builder.toString();
    }

    private static String appendPath(String path, String segment) {
        return path + "/" + segment.replace("~", "~0").replace("/", "~1");
    }

    private static void copyIncrement(PatchOperations target, String path, Object value) {
        if (value instanceof Double doubleValue) {
            target.increment(path, doubleValue);
        } else if (value instanceof Float floatValue) {
            target.increment(path, floatValue.doubleValue());
        } else if (value instanceof Number numberValue) {
            target.increment(path, numberValue.longValue());
        } else {
            throw new IllegalArgumentException("Unsupported increment value type: " + value);
        }
    }

    /**
     * Result of one sanitization pass.
     *
     * @param value sanitized value
     * @param removals removed U+0000 counts grouped by JSON Pointer path
     * @param <T> sanitized value type
     */
    public record SanitizationResult<T>(T value, List<Removal> removals) {
        public SanitizationResult {
            removals = List.copyOf(removals);
        }

        public boolean changed() {
            return !removals.isEmpty();
        }

        public int removedCount() {
            return removals.stream().mapToInt(Removal::count).sum();
        }
    }

    /**
     * Removal metadata for one JSON Pointer path.
     */
    public record Removal(String path, int count) {
    }
}
