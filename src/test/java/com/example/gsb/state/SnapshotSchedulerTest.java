package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 周期性快照与自动全量轮换。 */
class SnapshotSchedulerTest {

    @TempDir
    Path dir;

    @Test
    void periodicSnapshotsAreTriggeredAutomatically() throws Exception {
        List<SnapshotInfo> snapshots = new CopyOnWriteArrayList<>();
        CountDownLatch firstSnapshot = new CountDownLatch(1);
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 3);
             SnapshotScheduler scheduler = new SnapshotScheduler(backend, 80, 0,
                     info -> {
                         snapshots.add(info);
                         firstSnapshot.countDown();
                     })) {
            for (int i = 0; i < 100; i++) {
                backend.put("k" + i, "v" + i);
            }
            assertThat(firstSnapshot.await(10, TimeUnit.SECONDS)).isTrue();

            long deadline = System.currentTimeMillis() + 10_000;
            while (snapshots.size() < 4 && System.currentTimeMillis() < deadline) {
                backend.update("tick", (k, old) -> old == null ? "1" : old + "1");
                Thread.sleep(30);
            }
            assertThat(snapshots.size()).isGreaterThanOrEqualTo(4);
            // fullEveryN=3：链中应自动出现新的全量快照
            assertThat(snapshots.stream().map(SnapshotInfo::kind))
                    .contains(SnapshotInfo.Kind.FULL, SnapshotInfo.Kind.INCREMENTAL);
            assertThat(backend.stats().snapshotCount()).isGreaterThanOrEqualTo(4);
        }
    }
}
