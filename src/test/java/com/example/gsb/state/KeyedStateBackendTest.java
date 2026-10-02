package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 状态读写与事件位点。 */
class KeyedStateBackendTest {

    @TempDir
    Path dir;

    private KeyedStateBackend<String, String> backend() {
        return KeyedStateBackend.create(dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5);
    }

    @Test
    void putAndGetRoundTrip() {
        try (KeyedStateBackend<String, String> backend = backend()) {
            assertThat(backend.get("missing")).isNull();

            backend.put("user-1", "alice");
            backend.put("user-2", "bob");

            assertThat(backend.get("user-1")).isEqualTo("alice");
            assertThat(backend.get("user-2")).isEqualTo("bob");
            assertThat(backend.size()).isEqualTo(2);

            backend.put("user-1", "alice-v2");
            assertThat(backend.get("user-1")).isEqualTo("alice-v2");

            assertThat(backend.remove("user-2")).isTrue();
            assertThat(backend.get("user-2")).isNull();
            assertThat(backend.remove("user-2")).isFalse();
        }
    }

    @Test
    void processEventAdvancesPositionAndRejectsDuplicates() {
        try (KeyedStateBackend<String, String> backend = backend()) {
            assertThat(backend.processEvent("k", 1, (k, old) -> "v1"))
                    .isEqualTo(KeyedStateBackend.ProcessResult.APPLIED);
            assertThat(backend.processEvent("k", 2, (k, old) -> old + "+v2"))
                    .isEqualTo(KeyedStateBackend.ProcessResult.APPLIED);

            assertThat(backend.position()).isEqualTo(2);
            assertThat(backend.get("k")).isEqualTo("v1+v2");

            // 重复事件与乱序旧事件都被拒绝，状态与位点不变
            assertThat(backend.processEvent("k", 2, (k, old) -> "corrupted"))
                    .isEqualTo(KeyedStateBackend.ProcessResult.DUPLICATE);
            assertThat(backend.processEvent("k", 1, (k, old) -> "corrupted"))
                    .isEqualTo(KeyedStateBackend.ProcessResult.DUPLICATE);
            assertThat(backend.get("k")).isEqualTo("v1+v2");
            assertThat(backend.position()).isEqualTo(2);
        }
    }

    @Test
    void advancePositionOnlyMovesForward() {
        try (KeyedStateBackend<String, String> backend = backend()) {
            backend.advancePosition(10);
            backend.advancePosition(5);
            assertThat(backend.position()).isEqualTo(10);
            assertThat(backend.processEvent("k", 10, (k, old) -> "x"))
                    .isEqualTo(KeyedStateBackend.ProcessResult.DUPLICATE);
        }
    }

    @Test
    void statsExposeKeyCountPositionAndCleaning() {
        try (KeyedStateBackend<String, String> backend = backend()) {
            backend.processEvent("a", 1, (k, old) -> "1");
            backend.processEvent("b", 2, (k, old) -> "2");

            BackendStats stats = backend.stats();
            assertThat(stats.keyCount()).isEqualTo(2);
            assertThat(stats.position()).isEqualTo(2);
            assertThat(stats.snapshotCount()).isZero();
            assertThat(stats.totalKeysCleaned()).isZero();
            assertThat(stats.lastRecoveryMillis()).isEqualTo(-1);
        }
    }
}
