package com.example.gsb.state;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * 快照文件的读写与目录管理，对 {@link KeyedStateBackend} 屏蔽二进制格式细节。
 *
 * <p>单个快照文件布局（大端序）：
 * <pre>
 * magic(8)="GSBSTAT1" | version(int)=1 | kind(byte: 0=FULL,1=INC)
 * | id(long) | baseId(long) | position(long) | entryCount(int)
 * 重复 entryCount 次：
 *   op(byte: 0=UPSERT, 1=DELETE) | keyLen(int) | keyBytes
 *   UPSERT 额外：valLen(int) | valBytes | expireAt(long) | seq(long)
 *   DELETE 额外：seq(long)
 * </pre>
 * 数据先写 {@code .tmp} 再原子 rename，随后用同样方式更新 {@code LATEST} 指针文件，
 * 因此目录下不会出现半成品快照。
 */
final class SnapshotStore {

    static final byte OP_UPSERT = 0;
    static final byte OP_DELETE = 1;

    private static final byte[] MAGIC = "GSBSTAT1".getBytes();
    private static final int VERSION = 1;
    private static final String LATEST_FILE = "LATEST";
    private static final String SUFFIX_FULL = ".full";
    private static final String SUFFIX_INC = ".inc";

    private final Path dir;

    SnapshotStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
    }

    Path directory() {
        return dir;
    }

    /** 快照中的单条记录：UPSERT 携带完整状态，DELETE 是增量快照中的墓碑。 */
    record Entry(boolean delete, Object key, Object value, long expireAt, long seq) {
        static <K, V> Entry upsert(K key, V value, long expireAt, long seq) {
            return new Entry(false, key, value, expireAt, seq);
        }

        static <K> Entry delete(K key, long seq) {
            return new Entry(true, key, null, 0L, seq);
        }
    }

    /** 解析后的快照文件。 */
    record SnapshotFile(
            SnapshotInfo.Kind kind,
            long id,
            long baseId,
            long position,
            List<Entry> entries) {
    }

    <K, V> void write(SnapshotInfo.Kind kind, long id, long baseId, long position,
                      List<Entry> entries, StateCodec<K> keyCodec, StateCodec<V> valueCodec)
            throws IOException {
        Path tmp = dir.resolve(fileName(id, kind) + ".tmp");
        OutputStream raw = Files.newOutputStream(tmp);
        try (OutputStream out = new BufferedOutputStream(raw);
             DataOutputStream data = new DataOutputStream(out)) {
            data.write(MAGIC);
            data.writeInt(VERSION);
            data.writeByte(kind == SnapshotInfo.Kind.FULL ? 0 : 1);
            data.writeLong(id);
            data.writeLong(baseId);
            data.writeLong(position);
            data.writeInt(entries.size());
            for (Entry entry : entries) {
                data.writeByte(entry.delete() ? OP_DELETE : OP_UPSERT);
                byte[] keyBytes = keyCodec.encode(castKey(entry.key()));
                data.writeInt(keyBytes.length);
                data.write(keyBytes);
                if (!entry.delete()) {
                    byte[] valBytes = valueCodec.encode(castValue(entry.value()));
                    data.writeInt(valBytes.length);
                    data.write(valBytes);
                    data.writeLong(entry.expireAt());
                }
                data.writeLong(entry.seq());
            }
            data.flush();
            if (raw instanceof java.io.FileOutputStream fileOut) {
                fileOut.getFD().sync();
            }
        }
        Path target = dir.resolve(fileName(id, kind));
        atomicMove(tmp, target);

        Properties pointer = new Properties();
        pointer.setProperty("id", Long.toString(id));
        pointer.setProperty("kind", kind.name());
        pointer.setProperty("baseId", Long.toString(baseId));
        pointer.setProperty("position", Long.toString(position));
        Path pointerTmp = dir.resolve(LATEST_FILE + ".tmp");
        try (OutputStream out = Files.newOutputStream(pointerTmp)) {
            pointer.store(out, "latest state snapshot");
        }
        atomicMove(pointerTmp, dir.resolve(LATEST_FILE));
    }

    /**
     * 找到需要回放的快照链：最近的全量快照 + 之后按序号排列的全部增量快照。
     * 若 LATEST 指针损坏或缺失则扫描目录兜底；增量链不完整时回退到最新的可用全量快照。
     */
    <K, V> RecoveryChain<K, V> loadRecoveryChain(StateCodec<K> keyCodec, StateCodec<V> valueCodec)
            throws IOException {
        List<Located> located = scanSnapshots();
        if (located.isEmpty()) {
            return RecoveryChain.empty();
        }
        Located head = readPointer()
                .flatMap(ptr -> located.stream().filter(l -> l.id == ptr.id).findFirst())
                .orElse(located.get(located.size() - 1));

        List<Located> chain = new ArrayList<>();
        if (head.kind == SnapshotInfo.Kind.FULL) {
            chain.add(head);
        } else {
            Located base = null;
            for (Located l : located) {
                if (l.kind == SnapshotInfo.Kind.FULL && l.id == head.baseId) {
                    base = l;
                    break;
                }
            }
            if (base == null) {
                Located fallback = latestFull(located);
                if (fallback == null) {
                    return RecoveryChain.empty();
                }
                base = fallback;
            }
            chain.add(base);
            for (Located l : located) {
                if (l.kind == SnapshotInfo.Kind.INCREMENTAL
                        && l.id > base.id && l.id <= head.id) {
                    chain.add(l);
                }
            }
        }
        List<SnapshotFile> files = new ArrayList<>(chain.size());
        for (Located l : chain) {
            files.add(read(l.path, keyCodec, valueCodec));
        }
        SnapshotFile last = files.get(files.size() - 1);
        long fullId = files.get(0).id();
        return new RecoveryChain<>(files, last.position(), last.id(), fullId, files.size());
    }

    /** 新全量快照落盘成功后，删除它之前的所有快照文件，限制增量链长度。 */
    void deleteSnapshotsBefore(long fullId) throws IOException {
        for (Located l : scanSnapshots()) {
            if (l.id < fullId) {
                Files.deleteIfExists(l.path);
            }
        }
    }

    record RecoveryChain<K, V>(
            List<SnapshotFile> files,
            long position,
            long latestId,
            long baseFullId,
            int chainLength) {

        static <K, V> RecoveryChain<K, V> empty() {
            return new RecoveryChain<>(List.of(), 0L, 0L, 0L, 0);
        }

        boolean isEmpty() {
            return files.isEmpty();
        }
    }

    private record Pointer(long id) {
    }

    private record Located(SnapshotInfo.Kind kind, long id, long baseId, Path path) {
    }

    private List<Located> scanSnapshots() throws IOException {
        List<Located> result = new ArrayList<>();
        try (Stream<Path> paths = Files.list(dir)) {
            for (Path path : paths.toList()) {
                String name = path.getFileName().toString();
                SnapshotInfo.Kind kind;
                if (name.startsWith("snapshot-") && name.endsWith(SUFFIX_FULL)) {
                    kind = SnapshotInfo.Kind.FULL;
                } else if (name.startsWith("snapshot-") && name.endsWith(SUFFIX_INC)) {
                    kind = SnapshotInfo.Kind.INCREMENTAL;
                } else {
                    continue;
                }
                String idPart = name.substring("snapshot-".length(),
                        name.length() - (kind == SnapshotInfo.Kind.FULL
                                ? SUFFIX_FULL.length() : SUFFIX_INC.length()));
                long id;
                try {
                    id = Long.parseLong(idPart);
                } catch (NumberFormatException e) {
                    continue;
                }
                result.add(new Located(kind, id, -1L, path));
            }
        }
        result.sort(Comparator.comparingLong(Located::id));
        return result;
    }

    private Optional<Pointer> readPointer() {
        Path pointerFile = dir.resolve(LATEST_FILE);
        if (!Files.isRegularFile(pointerFile)) {
            return Optional.empty();
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(pointerFile)) {
            properties.load(in);
            return Optional.of(new Pointer(Long.parseLong(properties.getProperty("id"))));
        } catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
    }

    private Located latestFull(List<Located> located) {
        Located result = null;
        for (Located l : located) {
            if (l.kind == SnapshotInfo.Kind.FULL) {
                result = l;
            }
        }
        return result;
    }

    private <K, V> SnapshotFile read(Path path, StateCodec<K> keyCodec,
                                     StateCodec<V> valueCodec) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(path)))) {
            byte[] magic = new byte[MAGIC.length];
            in.readFully(magic);
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) {
                    throw new IOException("bad snapshot magic in " + path);
                }
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("unsupported snapshot version " + version + " in " + path);
            }
            SnapshotInfo.Kind kind = in.readByte() == 0
                    ? SnapshotInfo.Kind.FULL : SnapshotInfo.Kind.INCREMENTAL;
            long id = in.readLong();
            long baseId = in.readLong();
            long position = in.readLong();
            int count = in.readInt();
            List<Entry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                byte op = in.readByte();
                K key = keyCodec.decode(readBytes(in));
                if (op == OP_DELETE) {
                    long seq = in.readLong();
                    entries.add(Entry.delete(key, seq));
                } else if (op == OP_UPSERT) {
                    V value = valueCodec.decode(readBytes(in));
                    long expireAt = in.readLong();
                    long seq = in.readLong();
                    entries.add(Entry.upsert(key, value, expireAt, seq));
                } else {
                    throw new IOException("unknown entry op " + op + " in " + path);
                }
            }
            return new SnapshotFile(kind, id, baseId, position, entries);
        }
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            throw new IOException("negative byte length in snapshot");
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }

    private static String fileName(long id, SnapshotInfo.Kind kind) {
        return "snapshot-" + String.format("%019d", id)
                + (kind == SnapshotInfo.Kind.FULL ? SUFFIX_FULL : SUFFIX_INC);
    }

    private static void atomicMove(Path source, Path target) throws IOException {
        try {
            Files.move(source, target,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K> K castKey(Object key) {
        return (K) key;
    }

    @SuppressWarnings("unchecked")
    private static <V> V castValue(Object value) {
        return (V) value;
    }
}
