package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 并发更新：不同 key 互不阻塞，同一 key 更新严格有序。 */
class ConcurrentUpdateTest {

    @TempDir
    Path dir;

    @Test
    void disjointKeysAreUpdatedInParallelWithoutInterference() throws Exception {
        int threads = 8;
        int keysPerThread = 250;
        int updatesPerKey = 40;
        try (KeyedStateBackend<String, Long> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.int64(), 0L, 5)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                int threadId = t;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int k = 0; k < keysPerThread; k++) {
                        String key = "t" + threadId + "-k" + k;
                        for (int u = 0; u < updatesPerKey; u++) {
                            backend.update(key, (key1, old) -> (old == null ? 0L : old) + 1);
                        }
                    }
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            long expectedKeys = (long) threads * keysPerThread;
            assertThat(backend.size()).isEqualTo(expectedKeys);
            for (int t = 0; t < threads; t++) {
                for (int k = 0; k < keysPerThread; k++) {
                    // 每个 key 的更新次数精确等于期望值：无丢失、无串扰
                    assertThat(backend.get("t" + t + "-k" + k))
                            .isEqualTo((long) updatesPerKey);
                }
            }
        }
    }

    @Test
    void blockedUpdaterOnOneKeyDoesNotBlockOtherKeys() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 5)) {
            // 选两个落在不同哈希桶的 key，排除桶碰撞干扰
            String blockedKey = keyInBin(3);
            String freeKey = keyInBin(9);

            CountDownLatch updaterEntered = new CountDownLatch(1);
            CountDownLatch releaseUpdater = new CountDownLatch(1);
            ExecutorService pool = Executors.newSingleThreadExecutor();
            Future<?> blocked = pool.submit(() -> {
                backend.update(blockedKey, (k, old) -> {
                    updaterEntered.countDown();
                    try {
                        releaseUpdater.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "slow";
                });
                return null;
            });

            assertThat(updaterEntered.await(10, TimeUnit.SECONDS)).isTrue();
            // 另一个 key 的更新必须立即完成，不被阻塞中的 updater 拖住
            long startNanos = System.nanoTime();
            backend.update(freeKey, (k, old) -> "fast");
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
            assertThat(elapsedMillis).isLessThan(2_000);
            assertThat(backend.get(freeKey)).isEqualTo("fast");

            releaseUpdater.countDown();
            blocked.get(10, TimeUnit.SECONDS);
            pool.shutdown();
            assertThat(backend.get(blockedKey)).isEqualTo("slow");
        }
    }

    @Test
    void sameKeyUpdatesAreSerializedAndNeverLost() throws Exception {
        int threads = 8;
        int updatesPerThread = 5_000;
        try (KeyedStateBackend<String, Long> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.int64(), 0L, 5)) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        backend.update("hot", (k, old) -> (old == null ? 0L : old) + 1);
                    }
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // 同一 key 的所有更新串行执行：最终值必须精确等于更新总次数
            assertThat(backend.get("hot")).isEqualTo((long) threads * updatesPerThread);
        }
    }

    /** 找一个字符串，使其在 16 桶哈希表中落入指定桶（ConcurrentHashMap 初始容量）。 */
    private static String keyInBin(int bin) {
        for (int i = 0; ; i++) {
            String candidate = "bin-key-" + i;
            int hash = candidate.hashCode();
            int spread = hash ^ (hash >>> 16);
            if ((spread & 15) == bin) {
                return candidate;
            }
        }
    }
}
