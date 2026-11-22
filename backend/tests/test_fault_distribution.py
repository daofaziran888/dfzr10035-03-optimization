"""采集故障时间分布的关键口径回归测试。

全部用标准库 unittest，直接调用领域服务；重点是：
- 同一端点的失败/恢复必须靠真实调用编号关联；
- 没有恢复回执的窗口不算已结束；
- 迟到日志只形成一次更正，复算后只计一次；
- 归档重试不重复创建快照、不重复累计调用；
- 报告汇总与归档统计采用范围一致。
"""
from __future__ import annotations

import unittest

from app.services.fault_distribution import FaultDistributionService
from app.store import store


class FaultDistributionTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._original = {name: [dict(row) for row in rows] for name, rows in store._tables.items()}
        for module in ("fault_log", "fault_correction"):
            store._tables[module] = []
        for name, rows in store._tables.items():
            if name not in ("fault_log", "fault_correction"):
                store._tables[name] = [dict(row) for row in rows]
        self.service = FaultDistributionService()

    def tearDown(self) -> None:
        store._tables.clear()
        store._tables.update({name: [dict(row) for row in rows] for name, rows in self._original.items()})

    def ingest(self, endpoint: str, call_no: str, kind: str, moment: str, log_no: str | None = None):
        result = self.service.ingest({
            "端点": endpoint, "调用编号": call_no, "事件类型": kind,
            "发生时间": moment, "日志编号": log_no,
        })
        self.assertTrue(result["ok"], result)
        return result

    def test_failure_and_recovery_join_on_real_call(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        # 同端点另一笔调用的恢复不能提前结束 CALL-1 的窗口。
        self.ingest("ENDP-A", "CALL-2", "失败", "2026-11-22 09:06")
        self.ingest("ENDP-A", "CALL-2", "恢复", "2026-11-22 09:07")
        view = self.service.distribution("2026-11-22 09:00", "2026-11-22 10:00")
        windows = {row["调用编号"]: row for row in view["windows"]}
        self.assertEqual(windows["CALL-1"]["状态"], "进行中")
        self.assertIsNone(windows["CALL-1"]["结束时间"])
        self.assertEqual(windows["CALL-2"]["持续秒数"], 60)
        self.assertEqual(view["summary"]["进行中窗口数"], 1)

    def test_recovery_without_call_number_is_not_counted(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        result = self.service.ingest({
            "端点": "ENDP-A", "事件类型": "恢复", "发生时间": "2026-11-22 09:06",
        })
        self.assertFalse(result["ok"])
        view = self.service.distribution("2026-11-22 09:00", "2026-11-22 10:00")
        self.assertEqual(view["windows"][0]["状态"], "进行中")

    def test_ongoing_window_is_not_reported_as_ended(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})
        summary = report["report"]["fault_scope"]["summary"]
        self.assertEqual(summary["进行中窗口数"], 1)
        self.assertEqual(summary["已结束故障时长秒"], 0)
        self.assertTrue(summary["有窗口缺恢复回执"])

    def test_late_failure_shifts_start_and_is_counted_once(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05", "FLOG-A1")
        self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:11", "FLOG-A2")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]
        self.assertEqual(report["fault_scope"]["summary"]["已结束故障时长秒"], 360)

        late = self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:03", "FLOG-A3")
        self.assertTrue(late["late"])
        self.assertEqual(len(late["corrections"]), 1)
        self.assertEqual(late["corrections"][0]["更正类型"], "开始时间提前")

        recalculated = self.service.recalculate(report["报告编号"])["report"]
        scope = recalculated["fault_scope"]
        self.assertEqual(scope["summary"]["已结束故障时长秒"], 480)
        self.assertEqual(scope["summary"]["影响调用数"], 1)
        self.assertEqual(scope["window_snapshot"][0]["开始时间"], "2026-11-22 09:03:00")
        self.assertNotIn(3, scope["basis_log_ids"])

        # 再次复算不会把同一条更正再算一遍。
        again = self.service.recalculate(report["报告编号"])["report"]
        self.assertEqual(again["fault_scope"]["summary"]["已结束故障时长秒"], 480)

    def test_duplicate_log_is_ignored(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05", "FLOG-A1")
        duplicate = self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05", "FLOG-A1")
        self.assertTrue(duplicate["deduplicated"])
        self.assertEqual(len(store.rows("fault_log")), 1)

    def test_archive_is_idempotent_and_reconciles(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:11")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]

        first = self.service.archive_report(report["报告编号"])
        self.assertTrue(first["ok"])
        snapshots = [v for v in store.rows("data_version") if v.get("依赖版本") == report["报告编号"]]
        self.assertEqual(len(snapshots), 1)

        retry = self.service.archive_report(report["报告编号"])
        self.assertTrue(retry["reused"])
        self.assertFalse(retry["duplicate_snapshot_created"])
        self.assertEqual(
            len([v for v in store.rows("data_version") if v.get("依赖版本") == report["报告编号"]]), 1)

        check = self.service.consistency(report["报告编号"])
        self.assertTrue(check["consistent"])
        self.assertEqual(check["reconciliation"]["报告调用数"], 1)
        self.assertEqual(check["reconciliation"]["快照调用数"], 1)
        self.assertTrue(check["reconciliation"]["跨报告无重复"])

    def test_archive_retry_after_interruption_does_not_double_count(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:11")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:03")

        # 模拟上一轮归档：已建快照并应用更正，但状态没落到“已归档”就中断了。
        self.service._apply_corrections(
            report, self.service.report_fault_corrections(report), mark=True)
        versions = store.rows("data_version")
        versions.append({
            "id": 999, "版本编号": "DATA-FAULT-9999", "数据来源": "fault_distribution",
            "行数统计": 1, "依赖版本": report["报告编号"],
            "采用调用编号": ["CALL-1"], "窗口数": 1,
            "采用窗口键": report["fault_scope"]["window_keys"],
            "fault_scope_summary": report["fault_scope"]["summary"],
        })
        report["status"] = "已生成"

        result = self.service.archive_report(report["报告编号"])
        self.assertTrue(result["ok"])
        self.assertTrue(result["reused"])
        self.assertEqual(
            len([v for v in store.rows("data_version") if v.get("依赖版本") == report["报告编号"]]), 1)
        self.assertEqual(result["reconciliation"]["快照调用数"], 1)
        self.assertEqual(result["report"]["fault_scope"]["summary"]["影响调用数"], 1)
        self.assertTrue(result["reconciliation"]["采用范围一致"])

    def test_reports_do_not_double_adopt_same_call(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:11")
        first = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})
        self.assertTrue(first["ok"])
        second = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})
        self.assertFalse(second["ok"])
        self.assertIn("ENDP-A|CALL-1", second["excluded_already_adopted"])

        trace = self.service.trace("CALL-1")
        self.assertEqual(sum(trace["counting"]["报告计入次数"]), 1)
        self.assertTrue(trace["counting"]["恰好一次"])

    def test_late_log_destination_is_listed(self) -> None:
        self.ingest("ENDP-A", "CALL-1", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:11")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]
        late = self.ingest("ENDP-A", "CALL-1", "恢复", "2026-11-22 09:30")
        self.assertTrue(late["corrections"])
        corrections = self.service.corrections("CALL-1")
        self.assertEqual(corrections["total"], 1)
        destination = corrections["items"][0]["去向"]
        self.assertEqual(destination["报告编号"], report["报告编号"])
        self.assertFalse(destination["应用报告"])


if __name__ == "__main__":
    unittest.main()


class FaultWindowBoundaryTestCase(FaultDistributionTestCase):
    def test_late_recovery_moves_end_and_recomputes(self) -> None:
        self.ingest("ENDP-B", "CALL-9", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-B", "CALL-9", "恢复", "2026-11-22 09:20")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]
        self.assertEqual(report["fault_scope"]["summary"]["已结束故障时长秒"], 900)
        # 更早的恢复回执迟到：结束位置前移，当期故障时长缩短。
        late = self.ingest("ENDP-B", "CALL-9", "恢复", "2026-11-22 09:11")
        self.assertEqual(late["corrections"][0]["更正类型"], "结束时间调整")
        recalculated = self.service.recalculate(report["报告编号"])["report"]
        self.assertEqual(recalculated["fault_scope"]["summary"]["已结束故障时长秒"], 360)
        self.assertEqual(recalculated["fault_scope"]["window_snapshot"][0]["结束时间"], "2026-11-22 09:11:00")

    def test_window_crossing_period_start_counts_overlap_only(self) -> None:
        self.ingest("ENDP-C", "CALL-7", "失败", "2026-11-22 08:50")
        self.ingest("ENDP-C", "CALL-7", "恢复", "2026-11-22 09:10")
        view = self.service.distribution("2026-11-22 09:00", "2026-11-22 10:00")
        row = view["windows"][0]
        self.assertEqual(row["持续秒数"], 1200)
        self.assertEqual(view["summary"]["已结束故障时长秒"], 600)
        self.assertEqual(row["当期重叠秒数"], 600)

    def test_new_late_call_is_adopted_once(self) -> None:
        self.ingest("ENDP-D", "CALL-1", "失败", "2026-11-22 09:05")
        self.ingest("ENDP-D", "CALL-1", "恢复", "2026-11-22 09:11")
        report = self.service.generate_report({
            "时段开始": "2026-11-22 09:00", "时段结束": "2026-11-22 10:00"})["report"]
        late = self.ingest("ENDP-D", "CALL-2", "失败", "2026-11-22 09:30")
        self.assertTrue(late["late"])
        key = "ENDP-D|CALL-2"
        self.assertIsNone(late["previous_window"])
        recalculated = self.service.recalculate(report["报告编号"])["report"]
        self.assertIn(key, recalculated["fault_scope"]["window_keys"])
        self.assertEqual(recalculated["fault_scope"]["summary"]["影响调用数"], 2)
        trace = self.service.trace("CALL-2")
        self.assertEqual(sum(trace["counting"]["报告计入次数"]), 1)
