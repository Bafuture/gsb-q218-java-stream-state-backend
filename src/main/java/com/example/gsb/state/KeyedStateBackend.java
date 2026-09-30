package com.example.gsb.state;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A minimal keyed state backend for stream processing.
 *
 * <p>State lives in memory and is persisted to on-disk snapshots together with
 * the global processing position (the highest acknowledged event sequence).
 * Recovery loads the newest full snapshot, replays every newer incremental
 * snapshot, and resumes from the recorded position, so acknowledged events are
 * neither lost nor processed twice (assuming the event stream is ordered by
 * sequence number).</p>
 *
 * <h2>Concurrency model</h2>
 * <p>Updates are serialized per key through a stripe of {@link ReentrantLock}s:
 * threads touching different keys (different stripes) never block each other,
 * while updates to the same key are applied one at a time. Snapshotting copies
 * the state out of the concurrent map without holding any stripe lock, so disk
 * IO never blocks {@link #update(String, Serializable, long)}.</p>
 */
public class KeyedStateBackend implements AutoCloseable {

    private static final int DEFAULT_STRIPE_COUNT = 64;
    private static final Pattern FULL_FILE = Pattern.compile("snapshot-(\\d+)\\.full");
    private static final Pattern INCR_FILE = Pattern.compile("snapshot-(\\d+)\\.incr");

    private final Path snapshotDir;
    private final ConcurrentHashMap<String, Entry> state = new ConcurrentHashMap<>();
    private final Set<String> dirtyKeys = ConcurrentHashMap.newKeySet();
    private final ReentrantLock[] stripes;
    private final AtomicLong position = new AtomicLong(-1L);
    private volatile long restoredFloor = -1L;
    private final AtomicLong snapshotSeq = new AtomicLong(0L);
    private final StateMetrics metrics = new StateMetrics();
    private final LongSupplier clock;

    private ScheduledExecutorService snapshotScheduler;

    public KeyedStateBackend(Path snapshotDir) {
        this(snapshotDir, DEFAULT_STRIPE_COUNT, System::currentTimeMillis);
    }

    KeyedStateBackend(Path snapshotDir, int stripeCount, LongSupplier clock) {
        this.snapshotDir = snapshotDir;
        this.stripes = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            stripes[i] = new ReentrantLock();
        }
        this.clock = clock;
        try {
            Files.createDirectories(snapshotDir);
        } catch (IOException e) {
            throw new StateBackendException("cannot create snapshot directory " + snapshotDir, e);
        }
    }

    private ReentrantLock lockFor(String key) {
        return stripes[(key.hashCode() & 0x7fffffff) % stripes.length];
    }

    /**
     * Applies a state update for one event.
     *
     * <p>Duplicate detection is per key: an event is rejected when its sequence
     * number is not greater than the sequence of the last event applied to the
     * same key (or, for a key absent from state, the position recorded by the
     * snapshot this backend was restored from). This keeps keys independent of
     * each other while still guaranteeing that events acknowledged before a
     * crash are never processed twice after recovery.</p>
     *
     * @return {@code true} if the update was applied, {@code false} if the event
     *         was rejected as a duplicate or stale update for this key.
     */
    public boolean update(String key, Serializable value, long eventSeq) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            Entry existing = state.get(key);
            long lastSeq = existing != null ? existing.lastSeq : restoredFloor;
            if (eventSeq <= lastSeq) {
                metrics.recordRejectedDuplicate();
                return false;
            }
            long now = clock.getAsLong();
            long createdAt = existing == null ? now : existing.createdAt;
            state.put(key, new Entry(value, createdAt, now, eventSeq));
            dirtyKeys.add(key);
            position.accumulateAndGet(eventSeq, Math::max);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current value of a key, refreshing its last-access time so
     * that actively read state survives TTL cleanup.
     */
    public Optional<Serializable> get(String key) {
        Entry entry = state.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        entry.lastAccessAt = clock.getAsLong();
        return Optional.of(entry.value);
    }

    /** Highest acknowledged event sequence, or {@code -1} when nothing was processed. */
    public long getPosition() {
        return position.get();
    }

    public int keyCount() {
        return state.size();
    }

    public StateMetrics metrics() {
        return metrics;
    }

    /**
     * Takes a full snapshot: the complete state plus the current position.
     * Never holds state locks during disk IO.
     */
    public SnapshotInfo snapshot() {
        long id = snapshotSeq.incrementAndGet();
        drainDirtyKeys();
        Map<String, Entry> copy = new HashMap<>(state);
        long pos = position.get();
        Path file = writeSnapshotFile(SnapshotInfo.fullFileName(id), pos, copy, true);
        metrics.recordSnapshot(false);
        return new SnapshotInfo(id, id, pos, copy.size(), false, file);
    }

    /**
     * Takes an incremental snapshot containing only keys that changed since the
     * previous snapshot (deletions are written as tombstones).
     *
     * <p>The dirty set is drained before entries are read, so an update racing
     * with the snapshot is either captured here or re-marked dirty for the next
     * snapshot — a change can never fall through the cracks.</p>
     */
    public SnapshotInfo snapshotIncremental() {
        long id = snapshotSeq.incrementAndGet();
        List<String> changed = drainDirtyKeys();
        Map<String, Entry> delta = new HashMap<>();
        for (String key : changed) {
            delta.put(key, state.get(key));
        }
        long pos = position.get();
        Path file = writeSnapshotFile(SnapshotInfo.incrementalFileName(id), pos, delta, false);
        metrics.recordSnapshot(true);
        return new SnapshotInfo(id, latestFullIdOrZero(), pos, delta.size(), true, file);
    }

    private long latestFullIdOrZero() {
        try (Stream<Path> files = Files.list(snapshotDir)) {
            return files.map(p -> FULL_FILE.matcher(p.getFileName().toString()))
                    .filter(Matcher::matches)
                    .mapToLong(m -> Long.parseLong(m.group(1)))
                    .max()
                    .orElse(0L);
        } catch (IOException e) {
            throw new StateBackendException("cannot scan snapshot directory", e);
        }
    }

    private List<String> drainDirtyKeys() {
        List<String> drained = new ArrayList<>();
        Iterator<String> it = dirtyKeys.iterator();
        while (it.hasNext()) {
            drained.add(it.next());
            it.remove();
        }
        return drained;
    }

    private Path writeSnapshotFile(String fileName, long pos,
                                   Map<String, Entry> entries, boolean full) {
        Path target = snapshotDir.resolve(fileName);
        Path temp = snapshotDir.resolve(fileName + ".tmp");
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(temp))) {
            out.writeLong(pos);
            out.writeInt(entries.size());
            for (Map.Entry<String, Entry> e : entries.entrySet()) {
                out.writeObject(e.getKey());
                Entry entry = e.getValue();
                out.writeBoolean(entry != null);
                if (entry != null) {
                    out.writeObject(entry);
                }
            }
        } catch (IOException e) {
            throw new StateBackendException("failed writing snapshot " + fileName, e);
        }
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StateBackendException("failed committing snapshot " + fileName, e);
        }
        return target;
    }

    /**
     * Starts a daemon thread that snapshots periodically.
     *
     * @param intervalMillis period between snapshots
     * @param incremental    {@code true} for incremental snapshots, {@code false} for full ones
     */
    public void startPeriodicSnapshots(long intervalMillis, boolean incremental) {
        if (snapshotScheduler != null) {
            throw new IllegalStateException("periodic snapshots already started");
        }
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "state-backend-snapshot");
                    t.setDaemon(true);
                    return t;
                });
        executor.scheduleAtFixedRate(() -> {
            try {
                if (incremental) {
                    snapshotIncremental();
                } else {
                    snapshot();
                }
            } catch (RuntimeException ignored) {
                // a failed scheduled run must not kill the scheduler
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        snapshotScheduler = executor;
    }

    /**
     * Removes keys that have not been written or read for longer than {@code ttlMillis}.
     *
     * @return number of keys evicted
     */
    public int cleanupExpired(long ttlMillis) {
        long cutoff = clock.getAsLong() - ttlMillis;
        int[] removed = {0};
        state.entrySet().removeIf(e -> {
            boolean expired = e.getValue().lastAccessAt < cutoff;
            if (expired) {
                removed[0]++;
                dirtyKeys.add(e.getKey());
            }
            return expired;
        });
        metrics.recordEviction(removed[0]);
        return removed[0];
    }

    /**
     * Restores state from the newest snapshot chain: the latest full snapshot
     * plus every incremental snapshot with a higher id, replayed in order.
     *
     * @return the position to resume from; the next event must have a strictly
     *         greater sequence number
     */
    public long restore() {
        long startNanos = System.nanoTime();
        try {
            List<Path> chain = resolveSnapshotChain();
            if (chain.isEmpty()) {
                return -1L;
            }
            ConcurrentHashMap<String, Entry> restored = new ConcurrentHashMap<>();
            long restoredPosition = -1L;
            long maxId = 0L;
            for (Path file : chain) {
                String name = file.getFileName().toString();
                boolean full = name.endsWith(".full");
                Matcher matcher = (full ? FULL_FILE : INCR_FILE).matcher(name);
                if (!matcher.matches()) {
                    continue;
                }
                maxId = Math.max(maxId, Long.parseLong(matcher.group(1)));
                try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(file))) {
                    restoredPosition = in.readLong();
                    int count = in.readInt();
                    for (int i = 0; i < count; i++) {
                        String key = (String) in.readObject();
                        boolean present = in.readBoolean();
                        if (present) {
                            restored.put(key, (Entry) in.readObject());
                        } else {
                            restored.remove(key);
                        }
                    }
                } catch (IOException | ClassNotFoundException e) {
                    throw new StateBackendException("failed reading snapshot " + file, e);
                }
            }
            state.clear();
            state.putAll(restored);
            dirtyKeys.clear();
            position.set(restoredPosition);
            restoredFloor = restoredPosition;
            snapshotSeq.set(Math.max(snapshotSeq.get(), maxId));
            return restoredPosition;
        } finally {
            metrics.recordRestore(System.nanoTime() - startNanos);
        }
    }

    private List<Path> resolveSnapshotChain() {
        try (Stream<Path> files = Files.list(snapshotDir)) {
            List<Path> all = files.filter(Files::isRegularFile).toList();
            long latestFull = all.stream()
                    .map(p -> FULL_FILE.matcher(p.getFileName().toString()))
                    .filter(Matcher::matches)
                    .mapToLong(m -> Long.parseLong(m.group(1)))
                    .max()
                    .orElse(-1L);
            if (latestFull < 0) {
                return List.of();
            }
            List<Path> chain = new ArrayList<>();
            for (Path p : all) {
                String name = p.getFileName().toString();
                Matcher full = FULL_FILE.matcher(name);
                Matcher incr = INCR_FILE.matcher(name);
                long id = -1;
                if (full.matches()) {
                    id = Long.parseLong(full.group(1));
                } else if (incr.matches()) {
                    id = Long.parseLong(incr.group(1));
                }
                if (id >= latestFull) {
                    chain.add(p);
                }
            }
            chain.sort(Comparator.comparing(p -> {
                Matcher m = FULL_FILE.matcher(p.getFileName().toString());
                if (!m.matches()) {
                    m = INCR_FILE.matcher(p.getFileName().toString());
                }
                return Long.parseLong(m.matches() ? m.group(1) : "-1");
            }));
            return chain;
        } catch (IOException e) {
            throw new StateBackendException("cannot scan snapshot directory", e);
        }
    }

    @Override
    public void close() {
        if (snapshotScheduler != null) {
            snapshotScheduler.shutdownNow();
            snapshotScheduler = null;
        }
    }

    /** One stored value together with its TTL timestamps. */
    public static final class Entry implements Serializable {
        private static final long serialVersionUID = 1L;

        final Serializable value;
        final long createdAt;
        final long lastSeq;
        volatile long lastAccessAt;

        Entry(Serializable value, long createdAt, long lastAccessAt, long lastSeq) {
            this.value = value;
            this.createdAt = createdAt;
            this.lastAccessAt = lastAccessAt;
            this.lastSeq = lastSeq;
        }

        public Serializable value() {
            return value;
        }

        public long createdAt() {
            return createdAt;
        }

        public long lastAccessAt() {
            return lastAccessAt;
        }

        /** Sequence number of the event that produced this entry. */
        public long lastSeq() {
            return lastSeq;
        }
    }
}
