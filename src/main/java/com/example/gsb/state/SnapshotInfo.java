package com.example.gsb.state;

import java.nio.file.Path;

/**
 * Metadata describing one completed snapshot.
 *
 * <p>A full snapshot is self-contained. An incremental snapshot only contains
 * keys that changed since the previous snapshot and references the base full
 * snapshot id; recovery replays the latest full snapshot plus every
 * incremental snapshot newer than it, in snapshot-id order.</p>
 */
public record SnapshotInfo(long id,
                           long baseFullId,
                           long position,
                           int entryCount,
                           boolean incremental,
                           Path file) {

    static String fullFileName(long id) {
        return "snapshot-" + id + ".full";
    }

    static String incrementalFileName(long id) {
        return "snapshot-" + id + ".incr";
    }
}
