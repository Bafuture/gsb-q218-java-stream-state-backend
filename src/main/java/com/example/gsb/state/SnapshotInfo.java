package com.example.gsb.state;

import java.nio.file.Path;

/**
 * 一次成功快照的元信息。
 *
 * @param id         单调递增的快照编号
 * @param kind       全量或增量
 * @param baseId     增量快照基于的全量快照编号；全量快照为自身 id
 * @param position   快照覆盖到的事件位点
 * @param entryCount 本次写入的条目数（增量快照只含变化的 key）
 * @param file       快照文件路径
 */
public record SnapshotInfo(
        long id,
        Kind kind,
        long baseId,
        long position,
        int entryCount,
        Path file) {

    public enum Kind {
        FULL,
        INCREMENTAL
    }
}
