package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KeyedStateBackendTest {

    @TempDir
    Path snapshotDir;

    @Test
    void readsAndWritesStateAndTracksPosition() {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            assertThat(backend.update("user-1", "clicks=3", 1L)).isTrue();
            assertThat(backend.update("user-2", "clicks=7", 2L)).isTrue();

            assertThat(backend.get("user-1")).contains("clicks=3");
            assertThat(backend.get("user-2")).contains("clicks=7");
            assertThat(backend.get("missing")).isEmpty();
            assertThat(backend.getPosition()).isEqualTo(2L);
            assertThat(backend.keyCount()).isEqualTo(2);

            // overwrite keeps a single key
            assertThat(backend.update("user-1", "clicks=4", 3L)).isTrue();
            assertThat(backend.get("user-1")).contains("clicks=4");
            assertThat(backend.keyCount()).isEqualTo(2);
        }
    }

    @Test
    void rejectsAlreadyAcknowledgedEvents() {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            assertThat(backend.update("k", "v1", 10L)).isTrue();
            assertThat(backend.update("k", "v2", 10L)).isFalse();
            assertThat(backend.update("k", "v3", 5L)).isFalse();
            assertThat(backend.get("k")).contains("v1");
            assertThat(backend.metrics().getRejectedDuplicateCount()).isEqualTo(2);
        }
    }

    @Test
    void snapshotDoesNotBlockConcurrentUpdates() throws Exception {
        int keys = 2_000;
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            for (int i = 0; i < keys; i++) {
                backend.update("key-" + i, "v" + i, i);
            }
            AtomicLong seq = new AtomicLong(keys);
            AtomicBoolean stop = new AtomicBoolean();
            CountDownLatch snapshotStarted = new CountDownLatch(1);

            ExecutorService pool = Executors.newFixedThreadPool(2);
            Future<?> writer = pool.submit(() -> {
                while (!stop.get()) {
                    backend.update("hot-" + seq.get() % 8, "w", seq.getAndIncrement());
                }
            });
            Future<?> snapshotter = pool.submit(() -> {
                snapshotStarted.countDown();
                for (int i = 0; i < 20; i++) {
                    backend.snapshot();
                }
            });

            assertThat(snapshotStarted.await(5, TimeUnit.SECONDS)).isTrue();
            snapshotter.get(30, TimeUnit.SECONDS);
            stop.set(true);
            writer.get(30, TimeUnit.SECONDS);
            pool.shutdownNow();

            // every update the writer issued was accepted: nothing blocked or was lost
            assertThat(backend.getPosition()).isEqualTo(seq.get() - 1);
            assertThat(backend.metrics().getSnapshotCount()).isEqualTo(20);
            assertThat(backend.metrics().getFullSnapshotCount()).isEqualTo(20);
        }
    }

    @Test
    void periodicSnapshotsRunInBackground() throws Exception {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            backend.update("k", "v", 1L);
            backend.startPeriodicSnapshots(50, false);
            long deadline = System.currentTimeMillis() + 10_000;
            while (backend.metrics().getSnapshotCount() < 2
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertThat(backend.metrics().getSnapshotCount()).isGreaterThanOrEqualTo(2);
        }
    }

    @Test
    void restoresFromLatestSnapshotWithoutReprocessing() {
        long resumedPosition;
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            for (long seq = 1; seq <= 100; seq++) {
                assertThat(backend.update("key-" + (seq % 10), "v" + seq, seq)).isTrue();
            }
            backend.snapshot();
            // events after the snapshot are lost on crash, acknowledged ones are not
            resumedPosition = backend.getPosition();
        }

        try (KeyedStateBackend recovered = new KeyedStateBackend(snapshotDir)) {
            long position = recovered.restore();
            assertThat(position).isEqualTo(resumedPosition);
            assertThat(recovered.keyCount()).isEqualTo(10);
            assertThat(recovered.get("key-1")).contains("v91");
            assertThat(recovered.metrics().getLastRestoreTimeNanos()).isGreaterThan(0);

            // acknowledged events must not be processed twice
            for (long seq = 1; seq <= resumedPosition; seq++) {
                assertThat(recovered.update("key-" + (seq % 10), "dup" + seq, seq)).isFalse();
            }
            assertThat(recovered.metrics().getRejectedDuplicateCount())
                    .isEqualTo(resumedPosition);

            // consumption resumes exactly at the next event
            assertThat(recovered.update("key-1", "v101", 101L)).isTrue();
            assertThat(recovered.get("key-1")).contains("v101");
            assertThat(recovered.getPosition()).isEqualTo(101L);
        }
    }

    @Test
    void incrementalSnapshotWritesOnlyChangedKeysAndRestores() {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            for (int i = 0; i < 10; i++) {
                backend.update("key-" + i, "initial-" + i, i + 1);
            }
            SnapshotInfo full = backend.snapshot();
            assertThat(full.incremental()).isFalse();
            assertThat(full.entryCount()).isEqualTo(10);

            backend.update("key-3", "changed-3", 11);
            backend.update("key-7", "changed-7", 12);
            SnapshotInfo incr = backend.snapshotIncremental();
            assertThat(incr.incremental()).isTrue();
            assertThat(incr.entryCount()).isEqualTo(2);
            assertThat(incr.position()).isEqualTo(12L);
            assertThat(backend.metrics().getIncrementalSnapshotCount()).isEqualTo(1);

            // nothing changed since: next incremental is empty
            SnapshotInfo empty = backend.snapshotIncremental();
            assertThat(empty.entryCount()).isZero();
        }

        try (KeyedStateBackend recovered = new KeyedStateBackend(snapshotDir)) {
            long position = recovered.restore();
            assertThat(position).isEqualTo(12L);
            assertThat(recovered.keyCount()).isEqualTo(10);
            assertThat(recovered.get("key-3")).contains("changed-3");
            assertThat(recovered.get("key-7")).contains("changed-7");
            assertThat(recovered.get("key-0")).contains("initial-0");
        }
    }

    @Test
    void incrementalSnapshotCarriesTombstonesForExpiredKeys() throws Exception {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            backend.update("alive", "a", 1);
            backend.update("stale", "s", 2);
            backend.snapshot();
            Thread.sleep(10);
            assertThat(backend.cleanupExpired(0)).isEqualTo(2);
            backend.update("alive", "a2", 3);
            backend.snapshotIncremental();
        }

        try (KeyedStateBackend recovered = new KeyedStateBackend(snapshotDir)) {
            recovered.restore();
            assertThat(recovered.get("alive")).contains("a2");
            assertThat(recovered.get("stale")).isEmpty();
            assertThat(recovered.keyCount()).isEqualTo(1);
        }
    }

    @Test
    void cleanupExpiredEvictsStaleKeysAndCountsThem() throws Exception {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            backend.update("old-1", "v", 1);
            backend.update("old-2", "v", 2);
            Thread.sleep(30);
            backend.update("fresh", "v", 3);

            int evicted = backend.cleanupExpired(10);
            assertThat(evicted).isEqualTo(2);
            assertThat(backend.get("old-1")).isEmpty();
            assertThat(backend.get("old-2")).isEmpty();
            assertThat(backend.get("fresh")).contains("v");
            assertThat(backend.metrics().getEvictedKeyCount()).isEqualTo(2);

            // a read refreshes the access time and protects the key
            Thread.sleep(30);
            backend.get("fresh");
            assertThat(backend.cleanupExpired(10)).isZero();
            assertThat(backend.get("fresh")).contains("v");
        }
    }

    @Test
    void concurrentUpdatesOnDifferentKeysDoNotBlockEachOther() throws Exception {
        int threads = 8;
        int updatesPerThread = 500;
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < updatesPerThread; i++) {
                        long seq = (long) threadId * updatesPerThread + i + 1;
                        backend.update("key-" + threadId, "v" + i, seq);
                    }
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdownNow();

            assertThat(backend.keyCount()).isEqualTo(threads);
            for (int t = 0; t < threads; t++) {
                assertThat(backend.get("key-" + t)).contains("v" + (updatesPerThread - 1));
            }
            assertThat(backend.getPosition()).isEqualTo((long) threads * updatesPerThread);
        }
    }

    @Test
    void concurrentUpdatesOnSameKeyAreSerializedBySequence() throws Exception {
        int threads = 16;
        int updatesPerThread = 250;
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            AtomicLong nextSeq = new AtomicLong(0);
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    for (int i = 0; i < updatesPerThread; i++) {
                        long seq = nextSeq.incrementAndGet();
                        backend.update("shared", "seq-" + seq, seq);
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdownNow();

            long maxSeq = (long) threads * updatesPerThread;
            // the surviving value always belongs to the highest applied sequence
            Optional<java.io.Serializable> value = backend.get("shared");
            assertThat(value).isPresent();
            long winningSeq = Long.parseLong(value.get().toString().substring(4));
            assertThat(winningSeq).isEqualTo(backend.getPosition());
            assertThat(backend.getPosition()).isEqualTo(maxSeq);
        }
    }

    @Test
    void exposesMetrics() throws Exception {
        try (KeyedStateBackend backend = new KeyedStateBackend(snapshotDir)) {
            backend.update("a", "1", 1);
            backend.update("b", "2", 2);
            backend.snapshot();
            backend.update("a", "3", 3);
            backend.snapshotIncremental();
            Thread.sleep(10);
            backend.cleanupExpired(0);

            StateMetrics metrics = backend.metrics();
            assertThat(backend.keyCount()).isZero();
            assertThat(metrics.getSnapshotCount()).isEqualTo(2);
            assertThat(metrics.getFullSnapshotCount()).isEqualTo(1);
            assertThat(metrics.getIncrementalSnapshotCount()).isEqualTo(1);
            assertThat(metrics.getEvictedKeyCount()).isEqualTo(2);
        }

        try (KeyedStateBackend recovered = new KeyedStateBackend(snapshotDir)) {
            recovered.restore();
            assertThat(recovered.metrics().getLastRestoreTimeNanos()).isGreaterThan(0);
        }
    }
}
