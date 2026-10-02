package com.example.gsb.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 增量快照只写变化的 key，全量+增量链可正确恢复，删除以墓碑传播。 */
class IncrementalSnapshotTest {

    @TempDir
    Path dir;

    @Test
    void incrementalSnapshotContainsOnlyChangedKeys() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 10)) {
            backend.put("a", "1");
            backend.put("b", "1");
            backend.put("c", "1");
            assertThat(backend.snapshot(false).join().kind())
                    .isEqualTo(SnapshotInfo.Kind.FULL);

            backend.put("b", "2");
            backend.processEvent("d", 5, (k, old) -> "new");

            SnapshotInfo inc = backend.snapshot(true).join();
            assertThat(inc.kind()).isEqualTo(SnapshotInfo.Kind.INCREMENTAL);
            assertThat(inc.entryCount()).isEqualTo(2);
            assertThat(inc.baseId()).isEqualTo(1);
            assertThat(inc.position()).isEqualTo(5);

            // 无变化时再做增量，条目数为 0
            SnapshotInfo emptyInc = backend.snapshot(true).join();
            assertThat(emptyInc.kind()).isEqualTo(SnapshotInfo.Kind.INCREMENTAL);
            assertThat(emptyInc.entryCount()).isZero();
        }
    }

    @Test
    void fullPlusIncrementalChainRestoresCorrectStateIncludingDeletes() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 10)) {
            backend.put("a", "keep");
            backend.put("b", "remove-me");
            backend.put("c", "old");
            backend.snapshot(false).join();

            backend.remove("b");
            backend.put("c", "new");
            backend.put("e", "added");
            backend.advancePosition(7);
            backend.snapshot(true).join();

            backend.put("a", "keep-v2");
            backend.advancePosition(9);
            backend.snapshot(true).join();
        }

        try (KeyedStateBackend<String, String> restored = KeyedStateBackend.restore(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 10)) {
            assertThat(restored.position()).isEqualTo(9);
            assertThat(restored.get("a")).isEqualTo("keep-v2");
            assertThat(restored.get("b")).isNull();
            assertThat(restored.get("c")).isEqualTo("new");
            assertThat(restored.get("e")).isEqualTo("added");
            assertThat(restored.size()).isEqualTo(3);
        }
    }

    @Test
    void newFullSnapshotRetiresOldChain() throws Exception {
        try (KeyedStateBackend<String, String> backend = KeyedStateBackend.create(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 10)) {
            backend.put("a", "1");
            backend.snapshot(false).join();
            backend.put("a", "2");
            backend.snapshot(true).join();
            backend.put("a", "3");
            backend.snapshot(true).join();

            assertThat(backend.snapshot(false).join().kind())
                    .isEqualTo(SnapshotInfo.Kind.FULL);
            backend.put("a", "4");
            backend.snapshot(true).join();
        }

        try (Stream<Path> files = Files.list(dir)) {
            List<String> names = files
                    .map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".full") || n.endsWith(".inc"))
                    .sorted(Comparator.naturalOrder())
                    .toList();
            // 旧全量及其增量链已被清理
            assertThat(names).hasSize(2);
            assertThat(names.get(0)).endsWith(".full");
            assertThat(names.get(1)).endsWith(".inc");
        }

        try (KeyedStateBackend<String, String> restored = KeyedStateBackend.restore(
                dir, StateCodec.utf8String(), StateCodec.utf8String(), 0L, 10)) {
            assertThat(restored.get("a")).isEqualTo("4");
        }
    }
}
