package com.example.gsb.state;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * 流处理状态后端：按 key 保存状态、记录事件位点、周期性非阻塞快照、崩溃恢复、
 * 增量快照、TTL 过期清理与并发更新。
 *
 * <p>并发模型：状态表基于 {@link ConcurrentHashMap}，所有单 key 读写都通过
 * {@code compute}/{@code computeIfPresent} 完成，因此同一 key 的更新天然串行有序，
 * 不同 key 落在不同桶位上互不阻塞。快照的"收集"阶段只读内存、不持锁，编码与落盘
 * 在后台单线程执行，状态更新全程不被快照阻塞。
 *
 * <p>位点语义：{@link #processEvent} 按事件序号单调推进位点，重复或乱序的旧事件
 * （seq &lt;= 当前位点）直接判定为重复并跳过，保证重启后从快照位点续传时
 * 已确认事件不会被重复应用。
 *
 * <p>使用方约定：value 应为不可变对象（快照在后台编码）；事件流按 key 分区后
 * 序号单调递增。
 */
public final class KeyedStateBackend<K, V> implements AutoCloseable {

    /** {@link #processEvent} 的处理结果。 */
    public enum ProcessResult {
        /** 事件被接受，状态已更新、位点已推进。 */
        APPLIED,
        /** 事件序号不高于已确认位点，判定为重复，未产生任何副作用。 */
        DUPLICATE
    }

    /** 表内单条状态：值、过期时间与"本次写入对应的事件位点"。 */
    private record Holder<V>(V value, long expireAt, long seq) {
    }

    /** 待持久化的脏条目：UPSERT 或 DELETE（墓碑）。 */
    private record Pending(boolean delete, Object value, long expireAt, long seq) {
        static Pending upsert(Object value, long expireAt, long seq) {
            return new Pending(false, value, expireAt, seq);
        }

        static Pending delete(long seq) {
            return new Pending(true, null, 0L, seq);
        }
    }

    private final ConcurrentHashMap<K, Holder<V>> table = new ConcurrentHashMap<>();
    private final AtomicReference<ConcurrentHashMap<K, Pending>> dirty =
            new AtomicReference<>(new ConcurrentHashMap<>());
    private final ConcurrentHashMap<K, Long> tombstones = new ConcurrentHashMap<>();
    private final AtomicLong position;
    private final AtomicLong snapshotCount = new AtomicLong();
    private final AtomicLong totalKeysCleaned = new AtomicLong();
    private final AtomicLong lastRecoveryMillis = new AtomicLong(-1L);
    private final AtomicLong snapshotIdGen;
    private final AtomicLong lastFullSnapshotId;
    private final AtomicLong snapshotsSinceFull = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final SnapshotStore store;
    private final StateCodec<K> keyCodec;
    private final StateCodec<V> valueCodec;
    private final long defaultTtlMillis;
    private final int fullEveryN;
    private final ExecutorService snapshotWriter;

    private KeyedStateBackend(Path snapshotDir, StateCodec<K> keyCodec, StateCodec<V> valueCodec,
                              long defaultTtlMillis, int fullEveryN, long initialPosition)
            throws IOException {
        this.store = new SnapshotStore(snapshotDir);
        this.keyCodec = Objects.requireNonNull(keyCodec);
        this.valueCodec = Objects.requireNonNull(valueCodec);
        this.defaultTtlMillis = defaultTtlMillis;
        this.fullEveryN = Math.max(1, fullEveryN);
        this.position = new AtomicLong(initialPosition);
        this.snapshotIdGen = new AtomicLong();
        this.lastFullSnapshotId = new AtomicLong();
        this.snapshotWriter = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "state-snapshot-writer");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** 创建一个全新（空）状态后端。 */
    public static <K, V> KeyedStateBackend<K, V> create(
            Path snapshotDir, StateCodec<K> keyCodec, StateCodec<V> valueCodec,
            long defaultTtlMillis, int fullEveryN) {
        try {
            return new KeyedStateBackend<>(snapshotDir, keyCodec, valueCodec,
                    defaultTtlMillis, fullEveryN, 0L);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 从快照目录恢复：加载最近的全量快照并依序回放其后的增量快照，
     * 返回的后端位点即快照位点，可据此从事件源对应位置继续消费。
     */
    public static <K, V> KeyedStateBackend<K, V> restore(
            Path snapshotDir, StateCodec<K> keyCodec, StateCodec<V> valueCodec,
            long defaultTtlMillis, int fullEveryN) {
        long start = System.nanoTime();
        try {
            KeyedStateBackend<K, V> backend = new KeyedStateBackend<>(
                    snapshotDir, keyCodec, valueCodec, defaultTtlMillis, fullEveryN, 0L);
            SnapshotStore.RecoveryChain<K, V> chain =
                    backend.store.loadRecoveryChain(keyCodec, valueCodec);
            if (!chain.isEmpty()) {
                for (SnapshotStore.SnapshotFile file : chain.files()) {
                    for (SnapshotStore.Entry entry : file.entries()) {
                        @SuppressWarnings("unchecked")
                        K key = (K) entry.key();
                        if (entry.delete()) {
                            backend.table.remove(key);
                        } else {
                            @SuppressWarnings("unchecked")
                            V value = (V) entry.value();
                            backend.table.put(key, new Holder<>(value, entry.expireAt(), entry.seq()));
                        }
                    }
                }
                backend.position.set(chain.position());
                backend.snapshotIdGen.set(chain.latestId());
                backend.lastFullSnapshotId.set(chain.baseFullId());
            }
            backend.lastRecoveryMillis.set(
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            return backend;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------
    // 状态读写
    // ------------------------------------------------------------------

    /** 读取 key 的当前状态；不存在或已过期返回 {@code null}。 */
    public V get(K key) {
        Holder<V> holder = table.get(key);
        if (holder == null) {
            return null;
        }
        if (isExpired(holder.expireAt, System.currentTimeMillis())) {
            return null;
        }
        return holder.value();
    }

    /** 无条件写入（使用默认 TTL），不推进位点；适用于恢复回放或管理面写入。 */
    public void put(K key, V value) {
        Objects.requireNonNull(value, "value");
        long now = System.currentTimeMillis();
        long expireAt = expireAt(defaultTtlMillis, now);
        long seq = position.get();
        table.compute(key, (k, old) -> {
            dirty.get().put(k, Pending.upsert(value, expireAt, seq));
            return new Holder<>(value, expireAt, seq);
        });
    }

    /**
     * 处理一条事件：仅当 {@code seq} 大于当前位点时应用 {@code updater} 并推进位点，
     * 否则视为重复事件直接跳过。对同一 key 的并发调用按桶位锁串行化，保证有序。
     */
    public ProcessResult processEvent(K key, long seq,
                                      BiFunction<? super K, ? super V, ? extends V> updater) {
        Objects.requireNonNull(updater, "updater");
        if (seq <= position.get()) {
            return ProcessResult.DUPLICATE;
        }
        long now = System.currentTimeMillis();
        long expireAt = expireAt(defaultTtlMillis, now);
        ProcessResult[] result = new ProcessResult[1];
        table.compute(key, (k, old) -> {
            if (seq <= position.get()) {
                result[0] = ProcessResult.DUPLICATE;
                return old;
            }
            V newValue = Objects.requireNonNull(
                    updater.apply(k, old == null ? null : old.value()),
                    "updater must not return null");
            position.set(seq);
            dirty.get().put(k, Pending.upsert(newValue, expireAt, seq));
            result[0] = ProcessResult.APPLIED;
            return new Holder<>(newValue, expireAt, seq);
        });
        return result[0];
    }

    /**
     * 对单个 key 做原子读改写（不推进位点，适用于无事件序号的状态合并）。
     * 对同一 key 的并发调用按桶位锁严格串行，updater 总能看到上一次更新的结果；
     * 不同 key 的调用互不阻塞。
     */
    public V update(K key, BiFunction<? super K, ? super V, ? extends V> updater) {
        Objects.requireNonNull(updater, "updater");
        long now = System.currentTimeMillis();
        long expireAt = expireAt(defaultTtlMillis, now);
        long seq = position.get();
        V[] result = castArray(new Object[1]);
        table.compute(key, (k, old) -> {
            V newValue = Objects.requireNonNull(
                    updater.apply(k, old == null ? null : old.value()),
                    "updater must not return null");
            dirty.get().put(k, Pending.upsert(newValue, expireAt, seq));
            result[0] = newValue;
            return new Holder<>(newValue, expireAt, seq);
        });
        return result[0];
    }

    @SuppressWarnings("unchecked")
    private static <V> V[] castArray(Object[] array) {
        return (V[]) array;
    }

    /** 显式推进位点（如处理无状态事件或心跳），只允许向前。 */
    public void advancePosition(long seq) {
        position.accumulateAndGet(seq, Math::max);
    }

    /** 当前已确认处理的事件位点。 */
    public long position() {
        return position.get();
    }

    /** 删除 key，并在增量快照中留下墓碑，保证恢复后不会"复活"。 */
    public boolean remove(K key) {
        long seq = position.get();
        boolean[] removed = new boolean[1];
        table.computeIfPresent(key, (k, old) -> {
            removed[0] = true;
            dirty.get().put(k, Pending.delete(seq));
            tombstones.put(k, seq);
            return null;
        });
        return removed[0];
    }

    /** 当前存活（未过期）的 key 数。 */
    public long size() {
        long now = System.currentTimeMillis();
        long count = 0;
        for (Holder<V> holder : table.values()) {
            if (!isExpired(holder.expireAt, now)) {
                count++;
            }
        }
        return count;
    }

    // ------------------------------------------------------------------
    // 过期清理
    // ------------------------------------------------------------------

    /**
     * 清理所有已过期的 key，返回本次清理数量并累计到统计中。
     * 清理同样会生成墓碑，确保之后的增量快照能把删除传播到磁盘。
     */
    public long cleanupExpired() {
        long now = System.currentTimeMillis();
        long seq = position.get();
        long cleaned = 0;
        for (Map.Entry<K, Holder<V>> entry : table.entrySet()) {
            K key = entry.getKey();
            Holder<V> holder = entry.getValue();
            if (!isExpired(holder.expireAt, now)) {
                continue;
            }
            boolean[] removed = new boolean[1];
            table.computeIfPresent(key, (k, current) -> {
                if (!isExpired(current.expireAt, now)) {
                    return current;
                }
                removed[0] = true;
                dirty.get().put(k, Pending.delete(seq));
                return null;
            });
            if (removed[0]) {
                cleaned++;
            }
        }
        totalKeysCleaned.addAndGet(cleaned);
        return cleaned;
    }

    // ------------------------------------------------------------------
    // 快照
    // ------------------------------------------------------------------

    /**
     * 触发一次快照。内存中的收集阶段同步完成（只读、不持锁、不阻塞更新），
     * 编码与落盘异步执行；返回的 future 在快照文件安全落盘后完成。
     *
     * @param incremental true 只写上次快照以来变化的 key；false 写全量。
     *                    当尚无任何全量快照时自动降级为全量。
     */
    public CompletableFuture<SnapshotInfo> snapshot(boolean incremental) {
        ensureOpen();
        long id = snapshotIdGen.incrementAndGet();
        boolean full = !incremental || lastFullSnapshotId.get() == 0L;
        long snapshotPosition = position.get();
        long baseId = full ? id : lastFullSnapshotId.get();

        ConcurrentHashMap<K, Pending> swapped =
                dirty.getAndSet(new ConcurrentHashMap<>());
        List<SnapshotStore.Entry> entries =
                full ? gatherFull(swapped, snapshotPosition)
                     : gatherIncremental(swapped, snapshotPosition);
        SnapshotInfo.Kind kind = full ? SnapshotInfo.Kind.FULL : SnapshotInfo.Kind.INCREMENTAL;
        List<SnapshotStore.Entry> payload = entries;

        return CompletableFuture.supplyAsync(() -> {
            try {
                store.write(kind, id, baseId, snapshotPosition, payload, keyCodec, valueCodec);
                if (full) {
                    lastFullSnapshotId.set(id);
                    snapshotsSinceFull.set(0);
                    store.deleteSnapshotsBefore(id);
                } else {
                    snapshotsSinceFull.incrementAndGet();
                }
                reconcileDirty(swapped, payload);
                snapshotCount.incrementAndGet();
                return new SnapshotInfo(id, kind, baseId, snapshotPosition,
                        payload.size(), store.directory()
                        .resolve(String.format("snapshot-%019d.%s", id, full ? "full" : "inc")));
            } catch (IOException e) {
                throw new UncheckedIOException("snapshot " + id + " failed", e);
            }
        }, snapshotWriter);
    }

    /** 是否需要切换到全量快照（连续增量次数达到阈值）。 */
    public boolean shouldTakeFullSnapshot() {
        return lastFullSnapshotId.get() == 0L
                || snapshotsSinceFull.get() >= fullEveryN - 1;
    }

    /**
     * 收集全量条目：只纳入 holder.seq <= 快照位点的条目，保证"快照状态不超过
     * 快照位点"，恢复后重放区间内的事件可安全重做。旧脏表与收集期间的并发更新
     * 全部合并回活的脏表，落盘成功后由 {@link #reconcileDirty} 清掉已固化的部分。
     */
    private List<SnapshotStore.Entry> gatherFull(
            ConcurrentHashMap<K, Pending> swapped, long snapshotPosition) {
        List<SnapshotStore.Entry> entries = new ArrayList<>(table.size());
        ConcurrentHashMap<K, Pending> current = dirty.get();
        for (Map.Entry<K, Holder<V>> entry : table.entrySet()) {
            Holder<V> holder = entry.getValue();
            if (holder.seq() <= snapshotPosition) {
                entries.add(SnapshotStore.Entry.upsert(
                        entry.getKey(), holder.value(), holder.expireAt(), holder.seq()));
            } else {
                current.merge(entry.getKey(),
                        Pending.upsert(holder.value(), holder.expireAt(), holder.seq()),
                        (a, b) -> a.seq() >= b.seq() ? a : b);
            }
        }
        for (Map.Entry<K, Pending> entry : swapped.entrySet()) {
            current.merge(entry.getKey(), entry.getValue(),
                    (a, b) -> a.seq() >= b.seq() ? a : b);
        }
        return entries;
    }

    /**
     * 收集增量条目：只写入 holder 位点不超过快照位点的脏记录；其余脏记录连同
     * 收集期间发生的并发更新合并回活的脏表，交给下一次快照，
     * 避免"既没写进这次快照、又丢了脏标记"的丢数据窗口。
     */
    private List<SnapshotStore.Entry> gatherIncremental(
            ConcurrentHashMap<K, Pending> swapped, long snapshotPosition) {
        List<SnapshotStore.Entry> entries = new ArrayList<>(swapped.size());
        ConcurrentHashMap<K, Pending> current = dirty.get();
        for (Map.Entry<K, Pending> entry : swapped.entrySet()) {
            K key = entry.getKey();
            Pending pending = entry.getValue();
            current.merge(key, pending, (a, b) -> a.seq() >= b.seq() ? a : b);
            if (pending.seq() <= snapshotPosition) {
                entries.add(pending.delete()
                        ? SnapshotStore.Entry.delete(key, pending.seq())
                        : SnapshotStore.Entry.upsert(key, pending.value(),
                        pending.expireAt(), pending.seq()));
            }
        }
        return entries;
    }

    /**
     * 快照落盘成功后，从活的脏表中移除"与本次写入完全一致（同 seq）"的记录；
     * 记录已被后续更新覆盖（seq 不同）时保留，等待下一次快照。
     */
    private void reconcileDirty(ConcurrentHashMap<K, Pending> swapped,
                                List<SnapshotStore.Entry> written) {
        Map<K, Long> writtenSeq = new HashMap<>(written.size() * 2);
        for (SnapshotStore.Entry entry : written) {
            @SuppressWarnings("unchecked")
            K key = (K) entry.key();
            writtenSeq.put(key, entry.seq());
        }
        ConcurrentHashMap<K, Pending> current = dirty.get();
        for (Map.Entry<K, Long> entry : writtenSeq.entrySet()) {
            current.computeIfPresent(entry.getKey(),
                    (k, pending) -> pending.seq() == entry.getValue() ? null : pending);
        }
    }

    // ------------------------------------------------------------------
    // 统计与生命周期
    // ------------------------------------------------------------------

    public BackendStats stats() {
        return new BackendStats(
                size(),
                snapshotCount.get(),
                lastRecoveryMillis.get(),
                totalKeysCleaned.get(),
                position.get());
    }

    public Path snapshotDirectory() {
        return store.directory();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("state backend is closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            snapshotWriter.shutdown();
            try {
                if (!snapshotWriter.awaitTermination(30, TimeUnit.SECONDS)) {
                    snapshotWriter.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                snapshotWriter.shutdownNow();
            }
        }
    }

    private static boolean isExpired(long expireAt, long now) {
        return expireAt != Long.MAX_VALUE && expireAt <= now;
    }

    private static long expireAt(long ttlMillis, long now) {
        if (ttlMillis <= 0) {
            return Long.MAX_VALUE;
        }
        long expireAt = now + ttlMillis;
        return expireAt < 0 ? Long.MAX_VALUE : expireAt;
    }
}
