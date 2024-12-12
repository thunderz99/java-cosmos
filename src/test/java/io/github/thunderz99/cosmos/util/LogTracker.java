package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

/**
 * A helper class to track log events for unit test
 * <p>
 * see: <a href="https://www.baeldung.com/junit-asserting-logs">junit asserting logs</a>
 * </p>
 */
public class LogTracker extends AppenderBase<ILoggingEvent> {
    private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

    @Override
    protected void append(ILoggingEvent event) {
        events.add(event);
    }

    public List<ILoggingEvent> getEvents() {
        return events;
    }

    public static LogTracker getInstance(Class<?> loggerName, Level level) {
        var logger = (Logger) LoggerFactory.getLogger(loggerName);
        var tracker = new LogTracker();
        logger.setLevel(level);
        logger.addAppender(tracker);
        tracker.start();
        return tracker;
    }

    public static LogTracker getInstance(Class<?> loggerName) {
        return getInstance(loggerName,Level.DEBUG);
    }
}
