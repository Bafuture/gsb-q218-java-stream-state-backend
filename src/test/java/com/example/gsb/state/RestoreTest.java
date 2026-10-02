package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 从最近快照恢复，并从对应位点续传，已确认事件不被重复处理。 */
class RestoreTest {

    @TempDir
    Path dir;

    /** 对每个事件序号只接受一次，模拟副作用去重计数。 */
    private static final class IdempotentConsumer {
        final Map<String, Long> countsBySeq = new HashMap<>();

        String apply(long seq, String oldValue) {
            Long seen = countsBySeq.getOrDefault(String.valueOf(seq), 0L);
            if (seen > 0) {
                throw new AssertionError("event " + seq + " processed twice");
            }
            countsBySeq.put(String.valueOf(seq), 1L);
            long base = oldValue == null ? 0L : Long.parseLong(oldValue);
            return String.valueOf(base + 1);
        }

        long appliedCount() {
            return countsBySeq.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    @Test
    void restoreFromLatestSnapshotAndResumeWithoutDuplicates() throws Exception {
        IdempotentConsumer consumer = new IdempotentConsumer();

        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            for (long seq = 1; seq <= 10; seq++) {
                long eventSeq = seq;
                backend.processEvent("counter", eventSeq,
                        (k, old) -> consumer.apply(eventSeq, old));
            }
            backend.put("profile", "alice");
            assertThat(backend.get("counter")).isEqualTo("10");

            CompletableFuture<SnapshotInfo> snapshot = backend.snapshot(false);
            SnapshotInfo info = snapshot.join();
            assertThat(info.entryCount()).isEqualTo(2);
        }

        // 模拟崩溃后重启：位点 10 之前的事件不应被再次应用
        IdempotentConsumer replayConsumer = new IdempotentConsumer();
        try (KeyedStateBackend<String, String> restored = KeyedStateBackend.restore(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            assertThat(restored.position()).isEqualTo(10);
            assertThat(restored.get("counter")).isEqualTo("10");
            assertThat(restored.get("profile")).isEqualTo("alice");
            assertThat(restored.stats().lastRecoveryMillis()).isGreaterThanOrEqualTo(0);
            assertThat(restored.stats().keyCount()).isEqualTo(2);

            // 重放旧事件：全部判重，无副作用
            for (long seq = 1; seq <= 10; seq++) {
                long eventSeq = seq;
                assertThat(restored.processEvent("counter", eventSeq,
                        (k, old) -> replayConsumer.apply(eventSeq, old)))
                        .isEqualTo(KeyedStateBackend.ProcessResult.DUPLICATE);
            }
            assertThat(replayConsumer.appliedCount()).isZero();
            assertThat(restored.get("counter")).isEqualTo("10");

            // 从快照位点继续消费新事件
            for (long seq = 11; seq <= 15; seq++) {
                long eventSeq = seq;
                assertThat(restored.processEvent("counter", eventSeq,
                        (k, old) -> consumer.apply(eventSeq, old)))
                        .isEqualTo(KeyedStateBackend.ProcessResult.APPLIED);
            }
            assertThat(restored.position()).isEqualTo(15);
            assertThat(restored.get("counter")).isEqualTo("15");
        }
        assertThat(consumer.appliedCount()).isEqualTo(15);
    }

    @Test
    void restoreEmptyDirectoryStartsFromPositionZero() {
        try (KeyedStateBackend<String, String> restored = KeyedStateBackend.restore(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            assertThat(restored.position()).isZero();
            assertThat(restored.size()).isZero();
        }
    }
}
