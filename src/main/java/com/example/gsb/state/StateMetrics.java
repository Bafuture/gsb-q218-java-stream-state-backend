package com.example.gsb.state;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Backend-level counters exposed for monitoring.
 */
public final class StateMetrics {

    private final AtomicLong snapshotCount = new AtomicLong();
    private final AtomicLong fullSnapshotCount = new AtomicLong();
    private final AtomicLong incrementalSnapshotCount = new AtomicLong();
    private final AtomicLong evictedKeyCount = new AtomicLong();
    private final AtomicLong rejectedDuplicateCount = new AtomicLong();
    private final AtomicLong lastRestoreTimeNanos = new AtomicLong();

    void recordSnapshot(boolean incremental) {
        snapshotCount.incrementAndGet();
        if (incremental) {
            incrementalSnapshotCount.incrementAndGet();
        } else {
            fullSnapshotCount.incrementAndGet();
        }
    }

    void recordEviction(int count) {
        evictedKeyCount.addAndGet(count);
    }

    void recordRejectedDuplicate() {
        rejectedDuplicateCount.incrementAndGet();
    }

    void recordRestore(long elapsedNanos) {
        lastRestoreTimeNanos.set(elapsedNanos);
    }

    public long getSnapshotCount() {
        return snapshotCount.get();
    }

    public long getFullSnapshotCount() {
        return fullSnapshotCount.get();
    }

    public long getIncrementalSnapshotCount() {
        return incrementalSnapshotCount.get();
    }

    public long getEvictedKeyCount() {
        return evictedKeyCount.get();
    }

    public long getRejectedDuplicateCount() {
        return rejectedDuplicateCount.get();
    }

    public long getLastRestoreTimeNanos() {
        return lastRestoreTimeNanos.get();
    }

    @Override
    public String toString() {
        return "StateMetrics{snapshotCount=" + snapshotCount
                + ", fullSnapshotCount=" + fullSnapshotCount
                + ", incrementalSnapshotCount=" + incrementalSnapshotCount
                + ", evictedKeyCount=" + evictedKeyCount
                + ", rejectedDuplicateCount=" + rejectedDuplicateCount
                + ", lastRestoreTimeNanos=" + lastRestoreTimeNanos + '}';
    }
}
