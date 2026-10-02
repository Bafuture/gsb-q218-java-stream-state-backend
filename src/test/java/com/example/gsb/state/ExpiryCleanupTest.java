package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 按 key 存活时间清理陈旧状态并统计清理量。 */
class ExpiryCleanupTest {

    @TempDir
    Path dir;

    @Test
    void expiredKeysAreCleanedAndCounted() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 50L, 5)) {
            backend.put("short-1", "a");
            backend.put("short-2", "b");
            backend.put("short-3", "c");

            Thread.sleep(120);
            assertThat(backend.get("short-1")).isNull();

            long cleaned = backend.cleanupExpired();
            assertThat(cleaned).isEqualTo(3);
            assertThat(backend.size()).isZero();
            assertThat(backend.stats().totalKeysCleaned()).isEqualTo(3);

            // 再次清理没有可清理项，计数不重复
            assertThat(backend.cleanupExpired()).isZero();
            assertThat(backend.stats().totalKeysCleaned()).isEqualTo(3);
        }
    }

    @Test
    void freshKeysSurviveCleanupAndExpiryIsRestorable() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 60L, 5)) {
            backend.put("stale", "x");
            Thread.sleep(100);
            backend.put("fresh", "y");

            assertThat(backend.cleanupExpired()).isEqualTo(1);
            assertThat(backend.get("fresh")).isEqualTo("y");
            assertThat(backend.get("stale")).isNull();

            // 过期删除通过增量快照的墓碑传播，恢复后不会复活
            backend.snapshot(false).join();
        }

        try (KeyedStateBackend<String, String> restored = KeyedStateBackend.restore(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 60L, 5)) {
            assertThat(restored.get("stale")).isNull();
            assertThat(restored.get("fresh")).isEqualTo("y");
            assertThat(restored.size()).isEqualTo(1);
        }
    }

    @Test
    void zeroTtlMeansNeverExpire() {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            backend.put("immortal", "v");
            assertThat(backend.cleanupExpired()).isZero();
            assertThat(backend.get("immortal")).isEqualTo("v");
        }
    }
}
