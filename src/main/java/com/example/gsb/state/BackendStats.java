package com.example.gsb.state;

/**
 * 状态后端运行指标的不可变快照。
 *
 * @param keyCount            当前存活的 key 数
 * @param snapshotCount       成功落盘的快照次数（全量 + 增量）
 * @param lastRecoveryMillis  最近一次从快照恢复耗时（毫秒），未恢复过为 -1
 * @param totalKeysCleaned    过期清理累计删除的 key 数
 * @param position            当前已确认处理的事件位点（序号）
 */
public record BackendStats(
        long keyCount,
        long snapshotCount,
        long lastRecoveryMillis,
        long totalKeysCleaned,
        long position) {
}
