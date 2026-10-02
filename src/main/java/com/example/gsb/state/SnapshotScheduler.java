package com.example.gsb.state;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 周期性触发快照与过期清理的调度器。快照本身由
 * {@link KeyedStateBackend#snapshot(boolean)} 异步落盘，调度线程只负责触发，
 * 不会阻塞状态更新。连续增量达到后端阈值时自动切换为全量快照，以限制增量链长度。
 */
public final class SnapshotScheduler implements AutoCloseable {

    private final KeyedStateBackend<?, ?> backend;
    private final ScheduledExecutorService scheduler;
    private final Consumer<SnapshotInfo> snapshotListener;

    public SnapshotScheduler(KeyedStateBackend<?, ?> backend,
                             long snapshotIntervalMillis,
                             long cleanupIntervalMillis,
                             Consumer<SnapshotInfo> snapshotListener) {
        this.backend = Objects.requireNonNull(backend);
        this.snapshotListener = snapshotListener;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "state-snapshot-scheduler");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(this::triggerSnapshotSafely,
                snapshotIntervalMillis, snapshotIntervalMillis, TimeUnit.MILLISECONDS);
        if (cleanupIntervalMillis > 0) {
            scheduler.scheduleWithFixedDelay(this::cleanupSafely,
                    cleanupIntervalMillis, cleanupIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void triggerSnapshotSafely() {
        try {
            boolean full = backend.shouldTakeFullSnapshot();
            var future = backend.snapshot(!full);
            if (snapshotListener != null) {
                future.thenAccept(snapshotListener);
            }
        } catch (RuntimeException ignored) {
            // 后端已关闭等情况：调度器保持存活，等待 close。
        }
    }

    private void cleanupSafely() {
        try {
            backend.cleanupExpired();
        } catch (RuntimeException ignored) {
            // 同上。
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
