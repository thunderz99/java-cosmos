package io.github.thunderz99.cosmos.impl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClientLifecycleTest {

    private final Path tempDir;

    ClientLifecycleTest() throws java.io.IOException {
        tempDir = Files.createTempDirectory("java-cosmos-lifecycle-");
    }

    @AfterEach
    void removeTemporaryFiles() throws java.io.IOException {
        try (var files = Files.list(tempDir)) {
            for (var file : files.collect(java.util.stream.Collectors.toList())) {
                Files.deleteIfExists(file);
            }
        }
        Files.deleteIfExists(tempDir);
    }

    @Test
    void registration_is_once_and_successful_close_removes_hook() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(calls::incrementAndGet);
            lifecycle.registerShutdownHook();
            lifecycle.registerShutdownHook();

            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());
            lifecycle.close();
            lifecycle.close();
            hook.getValue().run();

            assertThat(calls.get()).isEqualTo(1);
            verify(runtime).removeShutdownHook(hook.getValue());
        }
    }

    @Test
    void concurrent_hook_and_explicit_close_wait_for_success() throws Exception {
        var runtime = mock(Runtime.class);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var secondEntered = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2);
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(() -> {
                calls.incrementAndGet();
                started.countDown();
                await(release);
            });
            lifecycle.registerShutdownHook();
            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());

            var hookCall = executor.submit(() -> hook.getValue().run());
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            var explicitCall = executor.submit(() -> {
                secondEntered.countDown();
                lifecycle.close();
            });
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> explicitCall.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
            release.countDown();
            hookCall.get(5, TimeUnit.SECONDS);
            explicitCall.get(5, TimeUnit.SECONDS);
            assertThat(calls.get()).isEqualTo(1);
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void failed_close_keeps_hook_and_can_be_retried() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        var failure = new IllegalStateException("close failed");
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(() -> {
                if (calls.incrementAndGet() == 1) {
                    throw failure;
                }
            });
            lifecycle.registerShutdownHook();
            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());

            assertThatThrownBy(lifecycle::close).isSameAs(failure);
            verify(runtime, never()).removeShutdownHook(hook.getValue());
            hook.getValue().run();
            lifecycle.close();

            assertThat(calls.get()).isEqualTo(2);
            verify(runtime).removeShutdownHook(hook.getValue());
        }
    }

    @Test
    void hook_failure_is_logged_without_claiming_success() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        var failure = new IllegalStateException("close failed");
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(() -> {
                calls.incrementAndGet();
                throw failure;
            });
            lifecycle.registerShutdownHook();
            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());
            hook.getValue().run();
            assertThatThrownBy(lifecycle::close).isSameAs(failure);
            assertThat(calls.get()).isEqualTo(2);
            verify(runtime, never()).removeShutdownHook(hook.getValue());
        }
    }

    @Test
    void removing_hook_during_shutdown_does_not_fail_successful_close() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(calls::incrementAndGet);
            lifecycle.registerShutdownHook();
            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());
            when(runtime.removeShutdownHook(hook.getValue())).thenThrow(new IllegalStateException("shutdown"));
            lifecycle.close();
            lifecycle.close();
            assertThat(calls.get()).isEqualTo(1);
            verify(runtime, times(1)).removeShutdownHook(hook.getValue());
        }
    }

    @Test
    void denied_hook_removal_does_not_fail_successful_close() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var lifecycle = new ClientLifecycle(calls::incrementAndGet);
            lifecycle.registerShutdownHook();
            var hook = ArgumentCaptor.forClass(Thread.class);
            verify(runtime).addShutdownHook(hook.capture());
            when(runtime.removeShutdownHook(hook.getValue())).thenThrow(new SecurityException("denied"));
            lifecycle.close();
            hook.getValue().run();
            assertThat(calls.get()).isEqualTo(1);
        }
    }

    @Test
    void registration_failure_leaves_no_hook_and_initialization_cleanup_preserves_original_failure() {
        var runtime = mock(Runtime.class);
        var calls = new AtomicInteger();
        var failure = new IllegalStateException("shutdown already started");
        try (var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            doThrow(failure).when(runtime).addShutdownHook(org.mockito.ArgumentMatchers.any(Thread.class));
            var lifecycle = new ClientLifecycle(calls::incrementAndGet);
            assertThatThrownBy(lifecycle::registerShutdownHook).isSameAs(failure);
            lifecycle.closeOnInitializationFailure(failure);
            assertThat(calls.get()).isEqualTo(1);
            assertThat(failure.getSuppressed()).isEmpty();
        }
    }

    @Test
    void cleanup_failure_is_suppressed_on_original_initialization_failure() {
        var failure = new IllegalArgumentException("initialization failed");
        var cleanupFailure = new IllegalStateException("cleanup failed");
        var lifecycle = new ClientLifecycle(() -> {
            throw cleanupFailure;
        });
        lifecycle.closeOnInitializationFailure(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
    }

    @Test
    void closed_client_cannot_register_a_hook() {
        var lifecycle = new ClientLifecycle(() -> {});
        lifecycle.close();
        assertThatThrownBy(lifecycle::registerShutdownHook).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void real_jvm_shutdown_closes_only_when_hook_is_registered() throws Exception {
        assertSubprocess("automatic", true);
        assertSubprocess("disabled", false);
        assertSubprocess("manual", true);
    }

    private void assertSubprocess(String mode, boolean expectedClose) throws Exception {
        var marker = tempDir.resolve(mode);
        var classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, ShutdownProbe.class.getName(), mode, marker.toString())
                .redirectErrorStream(true)
                .redirectOutput(tempDir.resolve(mode + ".log").toFile())
                .start();
        try {
            assertThat(process.waitFor(15, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).as(Files.readString(tempDir.resolve(mode + ".log"))).isZero();
            assertThat(Files.exists(marker)).isEqualTo(expectedClose);
            if (expectedClose) {
                assertThat(Files.readString(marker)).isEqualTo("closed\n");
            }
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for close release");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    public static class ShutdownProbe {
        public static void main(String[] args) {
            var lifecycle = new ClientLifecycle(() -> {
                try {
                    Files.writeString(Path.of(args[1]), "closed\n", java.nio.file.StandardOpenOption.CREATE,
                            java.nio.file.StandardOpenOption.APPEND);
                } catch (java.io.IOException failure) {
                    throw new IllegalStateException(failure);
                }
            });
            if (!"disabled".equals(args[0])) {
                lifecycle.registerShutdownHook();
            }
            if ("manual".equals(args[0])) {
                lifecycle.close();
                lifecycle.close();
            }
        }
    }
}
