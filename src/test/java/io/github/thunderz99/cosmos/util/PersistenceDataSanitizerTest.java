package io.github.thunderz99.cosmos.util;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import com.azure.cosmos.implementation.patch.PatchOperationCore;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PersistenceDataSanitizerTest {

    @Test
    void sanitizeDocumentShouldRemoveOnlyRealNulFromNestedStringValues() {
        var binary = new byte[]{0, 1, 2};
        var source = new LinkedHashMap<String, Object>();
        source.put("id", "id-1");
        source.put("plain", "literal \\u0000");
        source.put("controls", "line1\nline2\r\tend");
        source.put("nested", Map.of(
                "name", "A\u0000B\u0000C",
                "items", List.of("\u0000first", Map.of("value", "last\u0000"))));
        source.put("binary", binary);

        var result = PersistenceDataSanitizer.sanitizeDocument(source);

        assertThat(result.value())
                .containsEntry("plain", "literal \\u0000")
                .containsEntry("controls", "line1\nline2\r\tend");
        assertThat(result.value().get("binary")).isEqualTo(binary);
        assertThat(result.value())
                .extractingByKey("nested")
                .isEqualTo(Map.of(
                        "name", "ABC",
                        "items", List.of("first", Map.of("value", "last"))));
        assertThat(result.removals()).containsExactlyInAnyOrder(
                new PersistenceDataSanitizer.Removal("/nested/name", 2),
                new PersistenceDataSanitizer.Removal("/nested/items/0", 1),
                new PersistenceDataSanitizer.Removal("/nested/items/1/value", 1));
        assertThat(result.removedCount()).isEqualTo(4);
        assertThat(((Map<?, ?>) source.get("nested")).get("name")).isEqualTo("A\u0000B\u0000C");
    }

    @Test
    void sanitizeDocumentShouldBeIdempotentAndEscapeJsonPointerPaths() {
        var source = Map.of("a/b~c", "x\u0000y");

        var first = PersistenceDataSanitizer.sanitizeDocument(source);
        var second = PersistenceDataSanitizer.sanitizeDocument(first.value());

        assertThat(first.value()).containsEntry("a/b~c", "xy");
        assertThat(first.removals()).containsExactly(
                new PersistenceDataSanitizer.Removal("/a~1b~0c", 1));
        assertThat(second.changed()).isFalse();
    }

    @Test
    void sanitizeDocumentShouldNotChangeGeneralJsonConversion() {
        var source = Map.of("value", "A\u0000B");

        var generalMap = JsonUtil.toMap(source);
        var persistedMap = PersistenceDataSanitizer.sanitizeDocument(source).value();

        assertThat(generalMap).containsEntry("value", "A\u0000B");
        assertThat(persistedMap).containsEntry("value", "AB");
    }

    @Test
    void logIfChangedShouldContainOnlyMetadata() {
        var logger = (Logger) LoggerFactory.getLogger("PersistenceDataSanitizerTest.logger");
        var appender = new ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        var result = PersistenceDataSanitizer.sanitizeDocument(Map.of("secret", "private\u0000value"));

        try {
            PersistenceDataSanitizer.logIfChanged(
                    logger, "create", "Users", "tenant-1", "doc-1", result);
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(appender.list).singleElement().satisfies(event -> {
            assertThat(event.getFormattedMessage())
                    .contains("operation:create", "collection:Users", "partition:tenant-1",
                            "documentId:doc-1", "path:/secret", "removedCount:1")
                    .doesNotContain("private", "value");
        });
    }

    @Test
    void sanitizePatchOperationsShouldReturnSanitizedCopy() {
        var binary = new byte[]{0, 1, 2};
        var date = new Date(0);
        var source = PatchOperations.create()
                .set("/name", "A\u0000B")
                .add("/items", List.of(Map.of("value", "C\u0000D")))
                .replace("/plain", "literal \\u0000")
                .set("/binary", binary)
                .set("/date", date)
                .increment("/count", 1)
                .remove("/unused");

        var result = PersistenceDataSanitizer.sanitizePatchOperations(source);

        assertThat(result.value()).isNotSameAs(source);
        assertThat(result.value().size()).isEqualTo(source.size());
        assertThat(result.removals()).containsExactly(
                new PersistenceDataSanitizer.Removal("/name", 1),
                new PersistenceDataSanitizer.Removal("/items/0/value", 1));

        var originalName = (PatchOperationCore<?>) source.getPatchOperations().get(0);
        var sanitizedName = (PatchOperationCore<?>) result.value().getPatchOperations().get(0);
        assertThat(originalName.getResource()).isEqualTo("A\u0000B");
        assertThat(sanitizedName.getResource()).isEqualTo("AB");

        var sanitizedBinary = (PatchOperationCore<?>) result.value().getPatchOperations().get(3);
        var sanitizedDate = (PatchOperationCore<?>) result.value().getPatchOperations().get(4);
        assertThat(sanitizedBinary.getResource()).isSameAs(binary);
        assertThat(sanitizedDate.getResource()).isSameAs(date);
    }

    @Test
    void sanitizePatchOperationsShouldSanitizePojoAndArrayValues() {
        var source = PatchOperations.create()
                .set("/profile", new Profile("A\u0000B"))
                .set("/aliases", new String[]{"C\u0000D", "plain"});

        var result = PersistenceDataSanitizer.sanitizePatchOperations(source);

        var profile = (PatchOperationCore<?>) result.value().getPatchOperations().get(0);
        var aliases = (PatchOperationCore<?>) result.value().getPatchOperations().get(1);
        assertThat(profile.getResource()).isEqualTo(Map.of("name", "AB"));
        assertThat(aliases.getResource()).isEqualTo(List.of("CD", "plain"));
        assertThat(result.removedCount()).isEqualTo(2);
    }

    static class Profile {
        String name;

        Profile(String name) {
            this.name = name;
        }
    }
}
