package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 快照期间状态更新不被阻塞。 */
class SnapshotNonBlockingTest {

    @TempDir
    Path dir;

    @Test
    void snapshotCaptureDoesNotBlockStateUpdates() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            for (int i = 0; i < 20_000; i++) {
                backend.put("key-" + i, "value-" + i);
            }

            // 快照收集（同步阶段）必须快速返回，不等待磁盘 IO
            long startNanos = System.nanoTime();
            CompletableFuture<SnapshotInfo> future = backend.snapshot(false);
            long captureMillis = (System.nanoTime() - startNanos) / 1_000_000;
            assertThat(captureMillis).isLessThan(2_000);

            // 落盘仍在进行时，更新必须能继续推进
            List<String> writtenDuringSnapshot = new ArrayList<>();
            for (int i = 0; i < 2_000 && !future.isDone(); i++) {
                String key = "concurrent-" + i;
                backend.put(key, "during-" + i);
                writtenDuringSnapshot.add(key);
            }
            SnapshotInfo info = future.get(30, TimeUnit.SECONDS);
            assertThat(info.kind()).isEqualTo(SnapshotInfo.Kind.FULL);
            assertThat(info.entryCount()).isEqualTo(20_000);
            assertThat(backend.stats().snapshotCount()).isEqualTo(1);

            // 快照期间写入的数据在内存中完好
            for (String key : writtenDuringSnapshot) {
                assertThat(backend.get(key)).isNotNull();
            }
        }
    }

    @Test
    void updatesProceedWhileSnapshotFileIsWritten() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            for (int i = 0; i < 50_000; i++) {
                backend.put("bulk-" + i, "payload-" + i);
            }
            CompletableFuture<SnapshotInfo> future = backend.snapshot(false);

            CountDownLatch done = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                for (int i = 0; i < 5_000; i++) {
                    backend.update("hot-key", (k, old) -> old == null ? "1" : old + "1");
                }
                done.countDown();
            });
            writer.start();

            // 无论落盘是否结束，5_000 次单 key 更新都应在很短时间内完成
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            future.get(30, TimeUnit.SECONDS);
            writer.join();
            assertThat(backend.get("hot-key")).hasSize(5_000);
            assertThat(backend.stats().lastRecoveryMillis()).isEqualTo(-1);
        }
    }
}
