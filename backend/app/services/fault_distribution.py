"""采集故障时间分布领域服务。

围绕“采集失败 / 采集恢复”日志，按端点和真实调用编号还原故障窗口，
并给治理报告提供可复算、可归档的稳定口径。

关键约束（与值班交接口径一致）：
- 故障窗口只按“同一端点 + 同一调用编号”的日志还原，不对端点做累加；
- 窗口开始取该调用最早的失败日志，结束取该调用之后最早的恢复回执；
- 没有恢复回执的窗口保持“进行中”，不会被算成已经结束；
- 窗口按开始时间归属当期，持续时长按窗口与当期的重叠部分计算，
  因而迟到日志无论把开始还是结束挪动，都会重算当期数据；
- 报告生成时固化原依据（basis_log_ids + 窗口快照），迟到日志只形成
  一次性更正，复算时按日志编号去重；
- 归档幂等：中断后重试会先核对等待中的更正是否已经进入报告，
  复用同一份数据版本快照，不会重复累计同一笔调用。
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any

from app.store import store

FAILURE = "失败"
RECOVERY = "恢复"

_KIND_ALIASES = {
    "失败": FAILURE,
    "采集失败": FAILURE,
    "故障": FAILURE,
    "failure": FAILURE,
    "fail": FAILURE,
    "恢复": RECOVERY,
    "采集恢复": RECOVERY,
    "恢复回执": RECOVERY,
    "recovery": RECOVERY,
    "recover": RECOVERY,
}


def _pick(values: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in values and values[key] not in (None, ""):
            return values[key]
    return None


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ", timespec="seconds") if value else None


def _parse_ts(value: Any, fallback_date: date | None = None) -> datetime | None:
    """兼容 2026-11-22 09:05、2026-11-22T09:05、09:05 三种写法。"""
    if value in (None, ""):
        return None
    text = str(value).strip().replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        pass
    base = fallback_date or _now().date()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if fmt.startswith("%H"):
            parsed = parsed.replace(year=base.year, month=base.month, day=base.day)
        return parsed
    return None


def _normalise_kind(value: Any) -> str | None:
    text = str(value or "").strip()
    return _KIND_ALIASES.get(text) or _KIND_ALIASES.get(text.casefold())


def _seconds(start: datetime | None, end: datetime | None) -> int | None:
    if not start or not end:
        return None
    return max(0, int((end - start).total_seconds()))


class FaultDistributionService:
    """采集故障时间分布：日志录入、窗口还原、分布视图、报告与归档。"""

    LOG_MODULE = "fault_log"
    CORRECTION_MODULE = "fault_correction"
    REPORT_MODULE = "govern_report"
    VERSION_MODULE = "data_version"

    # ------------------------------------------------------------------ 基础
    def _logs(self) -> list[dict[str, Any]]:
        return store.rows(self.LOG_MODULE)

    def _corrections(self) -> list[dict[str, Any]]:
        return store.rows(self.CORRECTION_MODULE)

    def _reports(self) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(self.REPORT_MODULE)
            if isinstance(row.get("fault_scope"), dict)
        ]

    def _next_id(self, rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def _log_time(self, log: dict[str, Any]) -> datetime | None:
        return _parse_ts(log.get("发生时间"), None) or _parse_ts(log.get("到达时间"), None)

    def _log_ids(self, logs: list[dict[str, Any]]) -> set[int]:
        return {int(log["id"]) for log in logs if log.get("id") is not None}

    # -------------------------------------------------------------- 窗口还原
    def _build_windows(self, logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按 (端点, 调用编号) 还原窗口；不跨调用、不跨端点合并。"""
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for log in logs:
            if _normalise_kind(log.get("事件类型")) is None:
                continue
            endpoint = str(log.get("端点") or "").strip()
            call_no = str(log.get("调用编号") or "").strip()
            if not endpoint or not call_no:
                # 没有真实调用编号的日志无法关联窗口，留在未匹配清单里。
                continue
            groups.setdefault((endpoint, call_no), []).append(log)

        windows: list[dict[str, Any]] = []
        for (endpoint, call_no), items in groups.items():
            items = sorted(items, key=lambda row: (self._log_time(row) or datetime.max, int(row["id"])))
            failures = [
                row for row in items
                if _normalise_kind(row.get("事件类型")) == FAILURE and self._log_time(row)
            ]
            recoveries = [
                row for row in items
                if _normalise_kind(row.get("事件类型")) == RECOVERY and self._log_time(row)
            ]
            if not failures:
                # 只有恢复回执、没有失败锚点，不构成故障窗口。
                continue
            start = min(self._log_time(row) for row in failures)
            start_log = min(failures, key=lambda row: self._log_time(row))
            end = None
            end_log = None
            for row in sorted(recoveries, key=lambda item: self._log_time(item)):
                moment = self._log_time(row)
                if moment and moment >= start:
                    end = moment
                    end_log = row
                    break
            post_recovery = [
                int(row["id"]) for row in failures
                if end and self._log_time(row) > end
            ]
            duration = _seconds(start, end)
            windows.append({
                "窗口键": f"{endpoint}|{call_no}",
                "端点": endpoint,
                "调用编号": call_no,
                "开始时间": _iso(start),
                "结束时间": _iso(end),
                "状态": "已结束" if end else "进行中",
                "持续秒数": duration,
                "失败日志数": len(failures),
                "恢复日志数": len(recoveries),
                "失败日志编号": sorted(int(row["id"]) for row in failures),
                "恢复日志编号": sorted(int(row["id"]) for row in recoveries),
                "开始日志编号": int(start_log["id"]),
                "结束日志编号": int(end_log["id"]) if end_log else None,
                "窗口外失败日志编号": post_recovery,
                "_start": start,
                "_end": end,
            })
        windows.sort(key=lambda row: (row["_start"], row["窗口键"]))
        return windows

    def _public_window(self, window: dict[str, Any]) -> dict[str, Any]:
        return {key: copy.deepcopy(value) for key, value in window.items() if not key.startswith("_")}

    def _window_by_key(self, key: str, logs: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        for window in self._build_windows(logs if logs is not None else self._logs()):
            if window["窗口键"] == key:
                return window
        return None

    # ---------------------------------------------------------------- 时段计算
    def _resolve_period(
        self,
        values: dict[str, Any] | None = None,
        logs: list[dict[str, Any]] | None = None,
        windows: list[dict[str, Any]] | None = None,
    ) -> tuple[datetime, datetime]:
        values = values or {}
        anchors: list[datetime] = []
        for log in logs or []:
            moment = self._log_time(log)
            if moment:
                anchors.append(moment)
        for window in windows or []:
            anchors.append(window["_start"])
            if window.get("_end"):
                anchors.append(window["_end"])
        # 未显式给日期时，取最近一次日志的日期作为默认归属日，便于交班看当期。
        base_date = max(anchors).date() if anchors else _now().date()

        start = _parse_ts(_pick(values, "时段开始", "开始时间", "period_start"), base_date)
        end = _parse_ts(_pick(values, "时段结束", "结束时间", "period_end"), base_date)
        if start is None and end is None:
            start = datetime.combine(base_date, datetime.min.time())
            end = start + timedelta(days=1)
        elif start is None:
            start = datetime.combine(end.date(), datetime.min.time())
        elif end is None:
            end = start + timedelta(days=1)
        if end <= start:
            end = start + timedelta(days=1)
        return start, end

    def _clip_seconds(self, window: dict[str, Any], start: datetime, end: datetime) -> int:
        """计算已结束窗口与当期的重叠时长；进行中窗口不猜结束时间。"""
        if window.get("_end") is None:
            return 0
        left = max(window["_start"], start)
        right = min(window["_end"], end)
        return max(0, int((right - left).total_seconds()))

    def _overlaps(self, window: dict[str, Any], start: datetime, end: datetime) -> bool:
        """窗口与当期有重叠即纳入：开始/结束任一端移动都会改变当期口径。"""
        if window["_start"] >= end:
            return False
        return window.get("_end") is None or window["_end"] > start

    def _summarise(
        self,
        windows: list[dict[str, Any]],
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        in_period = [window for window in windows if self._overlaps(window, start, end)]
        ended = [window for window in in_period if window.get("_end") is not None]
        ongoing = [window for window in in_period if window.get("_end") is None]
        durations = [self._clip_seconds(window, start, end) for window in ended]
        unclipped = [int(window["持续秒数"] or 0) for window in ended]
        observed_ongoing = [
            max(0, int((end - max(window["_start"], start)).total_seconds()))
            for window in ongoing
        ]
        calls = sorted({window["调用编号"] for window in in_period})
        failure_logs = sum(len(window["失败日志编号"]) for window in in_period)
        recovery_logs = sum(len(window["恢复日志编号"]) for window in in_period)
        return {
            "时段开始": _iso(start),
            "时段结束": _iso(end),
            "窗口数": len(in_period),
            "已结束窗口数": len(ended),
            "进行中窗口数": len(ongoing),
            "影响调用数": len(calls),
            "影响调用编号": calls,
            "失败日志数": failure_logs,
            "恢复日志数": recovery_logs,
            "已结束故障时长秒": sum(durations),
            "未截断故障时长秒": sum(unclipped),
            "最长故障时长秒": max(unclipped, default=0),
            "进行中已观察时长秒": sum(observed_ongoing),
            "有窗口缺恢复回执": bool(ongoing),
            "归属规则": "窗口与当期有重叠即纳入；持续时长按与当期的重叠部分计；进行中窗口不猜结束时间。",
            "影响规模": {
                "调用数": len(calls),
                "窗口数": len(in_period),
                "失败日志数": failure_logs,
                "最长故障时长秒": max(unclipped, default=0),
            },
        }

    # ---------------------------------------------------------------- 分布视图
    def windows(
        self,
        period_start: Any = None,
        period_end: Any = None,
        endpoint: str | None = None,
        include_ongoing: bool = True,
    ) -> dict[str, Any]:
        logs = self._logs()
        start, end = self._resolve_period(
            {"时段开始": period_start, "时段结束": period_end}, logs)
        all_windows = self._build_windows(logs)
        in_period = [window for window in all_windows if self._overlaps(window, start, end)]
        if endpoint:
            in_period = [window for window in in_period if window["端点"] == endpoint]
        if not include_ongoing:
            in_period = [window for window in in_period if window.get("_end") is not None]
        rows = []
        for window in in_period:
            row = self._public_window(window)
            row["当期重叠秒数"] = self._clip_seconds(window, start, end)
            row["是否在当期结束"] = bool(window.get("_end") and window["_end"] <= end)
            rows.append(row)
        return {
            "period_start": _iso(start),
            "period_end": _iso(end),
            "items": rows,
            "total": len(rows),
            "summary": self._summarise(
                [window for window in all_windows if not endpoint or window["端点"] == endpoint],
                start, end),
            "unmatched_recoveries": self._unmatched_recoveries(logs, start, end),
        }

    def _unmatched_recoveries(
        self, logs: list[dict[str, Any]], start: datetime, end: datetime,
    ) -> list[dict[str, Any]]:
        """列出没有真实调用编号、无法关联窗口的恢复回执。"""
        rows = []
        for log in logs:
            if _normalise_kind(log.get("事件类型")) != RECOVERY:
                continue
            if str(log.get("调用编号") or "").strip():
                continue
            moment = self._log_time(log)
            if moment and start <= moment < end:
                rows.append({
                    "日志编号": log.get("日志编号"),
                    "端点": log.get("端点"),
                    "发生时间": log.get("发生时间"),
                    "原因": "恢复回执缺少真实调用编号，未计入任何窗口",
                })
        return rows

    def distribution(
        self,
        period_start: Any = None,
        period_end: Any = None,
        endpoint: str | None = None,
        bucket_minutes: int = 15,
    ) -> dict[str, Any]:
        logs = self._logs()
        start, end = self._resolve_period(
            {"时段开始": period_start, "时段结束": period_end}, logs)
        bucket_minutes = max(1, min(int(bucket_minutes or 15), 24 * 60))
        all_logs = logs
        if endpoint:
            all_logs = [log for log in logs if log.get("端点") == endpoint]
        windows = self._build_windows(all_logs)
        in_period = [window for window in windows if self._overlaps(window, start, end)]

        buckets: list[dict[str, Any]] = []
        cursor = start
        while cursor < end:
            slot_end = min(cursor + timedelta(minutes=bucket_minutes), end)
            buckets.append({
                "时段": f"{cursor.strftime('%H:%M')}-{slot_end.strftime('%H:%M')}",
                "开始时间": _iso(cursor),
                "结束时间": _iso(slot_end),
                "窗口数": 0,
                "已结束窗口数": 0,
                "进行中窗口数": 0,
                "失败日志数": 0,
                "恢复日志数": 0,
                "故障时长秒": 0,
                "进行中观察秒数": 0,
                "影响调用编号": [],
                "_start": cursor,
                "_end": slot_end,
                "_calls": set(),
            })
            cursor = slot_end

        def bucket_for(moment: datetime) -> dict[str, Any] | None:
            for bucket in buckets:
                if bucket["_start"] <= moment < bucket["_end"]:
                    return bucket
            return None

        for window in in_period:
            anchor = max(window["_start"], start)
            slot = bucket_for(anchor)
            if slot is None:
                continue
            slot["窗口数"] += 1
            slot["_calls"].add(window["调用编号"])
            if window.get("_end") is None:
                slot["进行中窗口数"] += 1
            else:
                slot["已结束窗口数"] += 1
            for bucket in buckets:
                left = max(window["_start"], bucket["_start"])
                if window.get("_end") is None:
                    # 进行中窗口只记录已观察时长，不当作已经结束的故障时长。
                    observed = max(0, int((min(end, bucket["_end"]) - left).total_seconds()))
                    bucket["进行中观察秒数"] += observed
                else:
                    right = min(window["_end"], bucket["_end"])
                    bucket["故障时长秒"] += max(0, int((right - left).total_seconds()))

        for log in all_logs:
            moment = self._log_time(log)
            if not moment or not (start <= moment < end):
                continue
            slot = bucket_for(moment)
            if slot is None:
                continue
            if _normalise_kind(log.get("事件类型")) == FAILURE:
                slot["失败日志数"] += 1
            elif _normalise_kind(log.get("事件类型")) == RECOVERY:
                slot["恢复日志数"] += 1

        items = []
        for bucket in buckets:
            row = {key: value for key, value in bucket.items() if not key.startswith("_")}
            row["影响调用编号"] = sorted(bucket["_calls"])
            row["影响调用数"] = len(bucket["_calls"])
            items.append(row)

        return {
            "period_start": _iso(start),
            "period_end": _iso(end),
            "bucket_minutes": bucket_minutes,
            "items": items,
            "windows": [
                {
                    **self._public_window(window),
                    "当期重叠秒数": self._clip_seconds(window, start, end),
                    "是否在当期结束": bool(window.get("_end") and window["_end"] <= end),
                }
                for window in in_period
            ],
            "summary": self._summarise(
                [window for window in windows if not endpoint or window["端点"] == endpoint],
                start, end),
            "boundary_rule": "窗口与当期有重叠即纳入；开始和结束位置都会改变当期持续时长；进行中窗口只展示已观察时长，不计为已结束。",
            "unmatched_recoveries": self._unmatched_recoveries(all_logs, start, end),
        }

    # ---------------------------------------------------------------- 日志录入
    def _report_keys(self) -> set[str]:
        keys: set[str] = set()
        for report in self._reports():
            keys.update(report.get("fault_scope", {}).get("window_keys", []))
        return keys

    def _reports_for_key(self, key: str) -> list[dict[str, Any]]:
        return [
            report for report in self._reports()
            if key in report.get("fault_scope", {}).get("window_keys", [])
        ]

    def _reports_covering(self, endpoint: str, moment: datetime) -> list[dict[str, Any]]:
        """找出评估时段覆盖该时刻、且采用过该端点窗口的报告。"""
        rows = []
        for report in self._reports():
            scope = report.get("fault_scope", {})
            start = _parse_ts(scope.get("period_start"))
            end = _parse_ts(scope.get("period_end"))
            if not start or not end or not (start <= moment < end):
                continue
            endpoints = {key.split("|", 1)[0] for key in scope.get("window_keys", [])}
            if endpoint in endpoints:
                rows.append(report)
        return rows

    def _fallback_date(self, endpoint: str, call_no: str) -> date | None:
        moments = [
            self._log_time(log) for log in self._logs()
            if log.get("端点") == endpoint and log.get("调用编号") == call_no
        ]
        moments = [moment for moment in moments if moment]
        return max(moments).date() if moments else None

    def ingest(self, values: dict[str, Any]) -> dict[str, Any]:
        endpoint = str(_pick(values, "端点", "接口端点", "目标端点") or "").strip()
        call_no = str(_pick(values, "调用编号", "调用记录编号", "记录编号") or "").strip()
        kind = _normalise_kind(_pick(values, "事件类型", "类型", "事件"))
        missing = []
        if not endpoint:
            missing.append("端点")
        if not call_no:
            missing.append("调用编号")
        if kind is None:
            missing.append("事件类型（失败/恢复）")
        if missing:
            return {"ok": False, "message": f"缺少必填字段：{'、'.join(missing)}"}

        log_no = str(_pick(values, "日志编号", "编号") or "").strip()
        logs = self._logs()
        if log_no:
            for log in logs:
                if log.get("日志编号") == log_no:
                    return {
                        "ok": True, "deduplicated": True,
                        "message": f"日志{log_no}已纳入，未重复计数",
                        "log": log,
                        "window": self._window_by_key(f"{endpoint}|{call_no}"),
                        "corrections": [],
                    }

        fallback = _parse_ts(_pick(values, "日期", "发生日期"), None)
        fallback_date = fallback.date() if fallback else self._fallback_date(endpoint, call_no)
        moment = _parse_ts(_pick(values, "发生时间", "时间", "采集时间", "事件时间"), fallback_date)
        if moment is None:
            return {"ok": False, "message": "无法解析发生时间，请使用 2026-11-22 09:05 或 09:05"}
        arrival = _parse_ts(_pick(values, "到达时间"), None) or _now()

        key = f"{endpoint}|{call_no}"
        previous_window = self._window_by_key(key)
        for log in logs:
            if (
                log.get("端点") == endpoint
                and log.get("调用编号") == call_no
                and _normalise_kind(log.get("事件类型")) == kind
                and self._log_time(log) == moment
            ):
                return {
                    "ok": True, "deduplicated": True,
                    "message": "同一端点、同一调用、同一时刻的日志已纳入，未重复计数",
                    "log": log,
                    "window": self._window_by_key(key),
                    "corrections": [],
                }

        previous_reports = self._reports_for_key(key)
        # 报告已经采用过这个时段的窗口时，新调用同样按迟到处理，否则当期口径会两边不一致。
        period_reports = [] if previous_reports else self._reports_covering(endpoint, moment)
        log = {
            "id": self._next_id(logs),
            "日志编号": log_no or None,
            "端点": endpoint,
            "调用编号": call_no,
            "事件类型": kind,
            "发生时间": _iso(moment),
            "采集批次": str(_pick(values, "采集批次", "批次") or ""),
            "来源系统": str(_pick(values, "来源系统", "来源") or ""),
            "到达时间": _iso(arrival),
            "迟到": False,
            "迟报原因": "",
            "status": kind,
            "pending": False,
            "abnormal": kind == FAILURE,
        }
        log["日志编号"] = log["日志编号"] or f"FLOG-{log['id']:04d}"
        adopted_reports = previous_reports or period_reports
        late = bool(adopted_reports) or bool(
            previous_window and moment < previous_window["_start"])
        if adopted_reports and previous_window is None:
            late_reason = "报告生成后到达，按历史调用编号新增窗口"
        elif previous_reports and arrival > moment:
            late_reason = "报告生成后到达，按原依据形成更正"
        elif previous_window and moment < previous_window["_start"]:
            late_reason = "早于已知失败时间到达，窗口开始前移"
        elif previous_reports:
            late_reason = "补充到已采用窗口"
        else:
            late_reason = ""
        log["迟到"] = late
        log["迟报原因"] = late_reason
        logs.append(log)

        current_window = self._window_by_key(key)
        corrections = []
        for report in previous_reports:
            corrections.append(self._create_correction(report, log, current_window, adopted=True))
        for report in period_reports:
            if log["调用编号"] in report["fault_scope"].get("adopted_call_numbers", []):
                continue
            corrections.append(self._create_correction(report, log, current_window, adopted=True))

        destinations = [
            {
                "报告编号": correction["报告编号"],
                "报告状态": correction["去向"].get("报告状态"),
                "更正编号": correction["更正编号"],
                "更正状态": correction["状态"],
            }
            for correction in corrections
        ]
        if not corrections:
            destinations.append({"报告编号": None, "结果": "仅进入故障分布视图，未被任何报告采用"})

        return {
            "ok": True,
            "deduplicated": False,
            "message": (
                f"已纳入{kind}日志{log['日志编号']}，"
                + ("形成迟到更正" if corrections else "无需更正，仅刷新分布视图")
            ),
            "late": late,
            "log": copy.deepcopy(log),
            "window": self._public_window(current_window) if current_window else None,
            "previous_window": self._public_window(previous_window) if previous_window else None,
            "corrections": corrections,
            "destinations": destinations,
        }

    # ------------------------------------------------------------------ 报告取用
    def _report_logs(
        self, report: dict[str, Any], extra_log_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        scope = report.get("fault_scope", {})
        allowed = {int(value) for value in scope.get("basis_log_ids", [])}
        applied = set(scope.get("applied_correction_ids", []))
        for correction in self._corrections():
            if correction.get("报告编号") == report.get("报告编号") and correction.get("id") in applied:
                allowed.add(int(correction["来源日志编号"]))
        if extra_log_ids:
            allowed.update(int(value) for value in extra_log_ids)
        return [log for log in self._logs() if int(log["id"]) in allowed]

    def _report_windows(self, report: dict[str, Any], extra_log_ids: set[int] | None = None) -> list[dict[str, Any]]:
        scope = report.get("fault_scope", {})
        keys = set(scope.get("window_keys", []))
        windows = self._build_windows(self._report_logs(report, extra_log_ids))
        return [window for window in windows if window["窗口键"] in keys]

    def _create_correction(
        self,
        report: dict[str, Any],
        log: dict[str, Any],
        current_window: dict[str, Any] | None,
        adopted: bool = False,
    ) -> dict[str, Any]:
        scope = report["fault_scope"]
        key = f"{log['端点']}|{log['调用编号']}"
        before = next(
            (item for item in scope.get("window_snapshot", []) if item.get("窗口键") == key),
            None,
        )
        after_windows = self._report_windows(report, {int(log["id"])})
        after = next((window for window in after_windows if window["窗口键"] == key), None)
        if after is None:
            after = self._window_by_key(key)
        public_after = self._public_window(after) if after else None

        old_start = _parse_ts(before.get("开始时间")) if before else None
        new_start = _parse_ts(after["开始时间"]) if after else None
        old_end = _parse_ts(before.get("结束时间")) if before else None
        new_end = _parse_ts(after["结束时间"]) if after else None

        if before is None and after is not None:
            kind = "新增窗口"
        elif after is None:
            kind = "移出当期"
        elif new_start and old_start and new_start < old_start:
            kind = "开始时间提前"
        elif old_end is None and new_end is not None:
            kind = "补齐恢复回执"
        elif new_end and old_end and new_end != old_end:
            kind = "结束时间调整"
        elif int(after["持续秒数"] or 0) != int(before.get("持续秒数") or 0):
            kind = "故障时长变更"
        else:
            kind = "日志补录"

        existing = next(
            (
                correction for correction in self._corrections()
                if correction.get("报告编号") == report.get("报告编号")
                and int(correction.get("来源日志编号", 0)) == int(log["id"])
            ),
            None,
        )
        if existing:
            # 同一笔迟到日志只形成一条更正；重复录入时沿用原更正，不新建也不重复计数。
            existing["新值"] = {
                "开始时间": public_after.get("开始时间") if public_after else None,
                "结束时间": public_after.get("结束时间") if public_after else None,
                "状态": public_after.get("状态") if public_after else None,
                "持续秒数": public_after.get("持续秒数") if public_after else None,
            }
            existing["状态"] = existing.get("状态", "待应用")
            existing["status"] = existing["状态"]
            existing["pending"] = existing["状态"] == "待应用"
            return copy.deepcopy(existing)

        # 迟到日志若为同一时段带来一笔新调用，按“历史调用编号”补入报告采用范围；
        # 若该调用已被其他报告采用，则留给那份报告，避免跨报告重复累计。
        other_adopters = [
            other for other in self._reports()
            if other.get("id") != report.get("id")
            and log["调用编号"] in other["fault_scope"].get("adopted_call_numbers", [])
        ]
        if adopted and after is not None and not other_adopters and key not in scope.get("window_keys", []):
            scope.setdefault("window_keys", []).append(key)
            if log["调用编号"] not in scope.setdefault("adopted_call_numbers", []):
                scope["adopted_call_numbers"].append(log["调用编号"])

        call_delta = 0
        if adopted and after is not None:
            before_calls = set(scope.get("adopted_call_numbers", []))
            if log["调用编号"] not in before_calls:
                call_delta = 1
        corrections = self._corrections()
        cid = self._next_id(corrections)
        status = "待应用"
        destination = {
            "报告编号": report.get("报告编号"),
            "报告状态": report.get("status"),
            "应用报告": False,
            "归档版本": [],
            "结果": (
                "报告已归档，更正待人工另立报告"
                if report.get("status") == "已归档"
                else "等待复算或归档时纳入报告"
            ),
        }
        correction = {
            "id": cid,
            "更正编号": f"FAULT-CORR-{cid:04d}",
            "端点": log["端点"],
            "调用编号": log["调用编号"],
            "窗口键": key,
            "来源日志编号": int(log["id"]),
            "来源日志编号展示": log.get("日志编号"),
            "来源日志时间": log.get("发生时间"),
            "事件类型": log.get("事件类型"),
            "报告编号": report.get("报告编号"),
            "更正类型": kind,
            "原值": {
                "开始时间": before.get("开始时间") if before else None,
                "结束时间": before.get("结束时间") if before else None,
                "状态": before.get("状态") if before else None,
                "持续秒数": before.get("持续秒数") if before else None,
            },
            "新值": {
                "开始时间": public_after.get("开始时间") if public_after else None,
                "结束时间": public_after.get("结束时间") if public_after else None,
                "状态": public_after.get("状态") if public_after else None,
                "持续秒数": public_after.get("持续秒数") if public_after else None,
            },
            "影响": {
                "持续秒数增量": int((public_after or {}).get("持续秒数") or 0) - int((before or {}).get("持续秒数") or 0),
                "调用数增量": call_delta,
                "窗口数增量": 0 if before and public_after else call_delta,
            },
            "状态": status,
            "去向": destination,
            "记录时间": _iso(_now()),
            "status": status,
            "pending": status == "待应用",
            "abnormal": False,
        }
        corrections.append(correction)
        return copy.deepcopy(correction)

    def generate_report(self, values: dict[str, Any] | None = None) -> dict[str, Any]:
        values = values or {}
        logs = self._logs()
        all_windows = self._build_windows(logs)
        start, end = self._resolve_period(values, logs, all_windows)
        in_period = [window for window in all_windows if self._overlaps(window, start, end)]

        adopted_keys: set[str] = set()
        for report in self._reports():
            adopted_keys.update(report["fault_scope"].get("window_keys", []))
        adoptable = [window for window in in_period if window["窗口键"] not in adopted_keys]
        excluded = [window["窗口键"] for window in in_period if window["窗口键"] in adopted_keys]
        if not adoptable:
            return {
                "ok": False,
                "message": "时段内没有可采用的故障窗口（可能尚未录入日志或已被其他报告采用）",
                "excluded_already_adopted": excluded,
            }

        summary = self._summarise(adoptable, start, end)
        scope = {
            "period_start": _iso(start),
            "period_end": _iso(end),
            "window_keys": [window["窗口键"] for window in adoptable],
            "basis_log_ids": sorted(
                set().union(*(set(window["失败日志编号"]) | set(window["恢复日志编号"]) for window in adoptable))
            ),
            "adopted_call_numbers": sorted({window["调用编号"] for window in adoptable}),
            "window_snapshot": [self._public_window(window) for window in adoptable],
            "applied_correction_ids": [],
            "summary": summary,
            "excluded_already_adopted": excluded,
            "adoption_rule": "窗口与时段重叠即纳入采用；一笔调用只由一份报告采用，避免跨报告重复累计。",
            "generated_at": _iso(_now()),
        }

        rows = store.rows(self.REPORT_MODULE)
        rid = self._next_id(rows)
        faults = [
            row for row in rows
            if str(row.get("报告编号", "")).startswith("GOVE-FAULT-")
        ]
        seq = len(faults) + 1
        score = 100.0
        score -= min(60.0, summary["已结束故障时长秒"] / 60.0 * 0.5)
        score -= summary["进行中窗口数"] * 15.0
        score -= summary["窗口数"] * 2.0
        report = {
            "id": rid,
            "报告编号": f"GOVE-FAULT-{seq:04d}",
            "评估范围": f"采集故障时间分布 {_iso(start)} ~ {_iso(end)}",
            "合规指标": "采集故障时间分布",
            "当前得分": round(max(0.0, score), 1),
            "改进事项": (
                f"故障窗口{summary['窗口数']}个（进行中{summary['进行中窗口数']}个），"
                f"影响调用{summary['影响调用数']}笔，"
                f"已结束故障时长{summary['已结束故障时长秒']}秒"
            ),
            "评估周期": str(_pick(values, "评估周期") or "2026-Q4"),
            "负责人": str(_pick(values, "负责人") or "值班SRE"),
            "生成时间": _now().date().isoformat(),
            "status": "已生成",
            "pending": False,
            "abnormal": summary["进行中窗口数"] > 0,
            "fault_scope": scope,
            "archive": {},
        }
        rows.append(report)
        return {
            "ok": True,
            "message": f"已生成治理报告{report['报告编号']}，采用{summary['窗口数']}个故障窗口",
            "report": copy.deepcopy(report),
            "summary": summary,
            "excluded_already_adopted": excluded,
        }

    def _resolve_report(self, ref: Any) -> dict[str, Any] | None:
        text = str(ref)
        for report in self._reports():
            if str(report.get("id")) == text or str(report.get("报告编号")) == text:
                return report
        return None

    def report_fault_corrections(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            correction for correction in self._corrections()
            if correction.get("报告编号") == report.get("报告编号")
        ]

    def _apply_corrections(
        self,
        report: dict[str, Any],
        corrections: list[dict[str, Any]],
        mark: bool = True,
    ) -> dict[str, Any]:
        scope = report["fault_scope"]
        applied = set(scope.get("applied_correction_ids", []))
        for correction in corrections:
            applied.add(int(correction["id"]))
        # 先登记更正编号，_report_windows 才能按更正把来源日志纳入原依据。
        scope["applied_correction_ids"] = sorted(applied)
        windows = self._report_windows(report)
        summary = self._summarise(windows, _parse_ts(scope["period_start"]), _parse_ts(scope["period_end"]))
        scope["window_snapshot"] = [self._public_window(window) for window in windows]
        scope["summary"] = summary
        scope["recalculated_at"] = _iso(_now())
        report["当前得分"] = round(max(0.0, 100.0
            - min(60.0, summary["已结束故障时长秒"] / 60.0 * 0.5)
            - summary["进行中窗口数"] * 15.0
            - summary["窗口数"] * 2.0), 1)
        report["改进事项"] = (
            f"故障窗口{summary['窗口数']}个（进行中{summary['进行中窗口数']}个），"
            f"影响调用{summary['影响调用数']}笔，"
            f"已结束故障时长{summary['已结束故障时长秒']}秒"
        )
        if mark:
            for correction in corrections:
                correction["状态"] = "已应用"
                correction["status"] = "已应用"
                correction["pending"] = False
                correction["应用时间"] = _iso(_now())
                correction["去向"]["应用报告"] = True
                correction["去向"]["报告状态"] = report.get("status")
                correction["去向"]["结果"] = "已按历史调用编号复算进报告"
        return summary

    def recalculate(self, ref: Any) -> dict[str, Any]:
        report = self._resolve_report(ref)
        if report is None:
            return {"ok": False, "message": f"治理报告{ref}不存在"}
        if report.get("status") == "已归档":
            return {"ok": False, "message": "报告已归档，不能再改写；迟到更正需另立报告"}
        pending = [
            correction for correction in self.report_fault_corrections(report)
            if correction["状态"] == "待应用"
        ]
        summary = self._apply_corrections(report, pending)
        return {
            "ok": True,
            "message": (
                f"报告{report['报告编号']}已按原依据复算，纳入{len(pending)}条更正"
                if pending else f"报告{report['报告编号']}已按原依据复算，无待应用更正"
            ),
            "report": copy.deepcopy(report),
            "summary": summary,
            "applied_correction_ids": list(report["fault_scope"]["applied_correction_ids"]),
        }

    # ---------------------------------------------------------------- 归档
    def _fingerprint(self, report: dict[str, Any]) -> str:
        scope = report["fault_scope"]
        payload = {
            "report": report.get("报告编号"),
            "window_keys": sorted(scope.get("window_keys", [])),
            "applied_correction_ids": sorted(scope.get("applied_correction_ids", [])),
            "summary": scope.get("summary", {}),
        }
        return hashlib.sha1(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]

    def _snapshot_for_report(self, report: dict[str, Any]) -> dict[str, Any] | None:
        for version in store.rows(self.VERSION_MODULE):
            if (
                str(version.get("版本编号", "")).startswith("DATA-FAULT-")
                and version.get("依赖版本") == report.get("报告编号")
            ):
                return version
        return None

    def _reconcile(self, report: dict[str, Any], snapshot: dict[str, Any] | None) -> dict[str, Any]:
        scope = report["fault_scope"]
        summary = scope.get("summary", {})
        snapshot_summary = (snapshot or {}).get("fault_scope_summary", {})
        calls = set(scope.get("adopted_call_numbers", []))
        snapshot_calls = set((snapshot or {}).get("采用调用编号", []))
        overlapping: dict[str, list[str]] = {}
        for other in self._reports():
            if other.get("id") == report.get("id"):
                continue
            other_calls = set(other["fault_scope"].get("adopted_call_numbers", []))
            shared = sorted(calls & other_calls)
            if shared:
                overlapping[str(other.get("报告编号"))] = shared
        return {
            "报告编号": report.get("报告编号"),
            "快照版本": (snapshot or {}).get("版本编号"),
            "报告窗口数": summary.get("窗口数"),
            "快照窗口数": (snapshot or {}).get("窗口数"),
            "报告调用数": summary.get("影响调用数"),
            "快照调用数": (snapshot or {}).get("行数统计"),
            "报告采用调用编号": sorted(calls),
            "快照采用调用编号": sorted(snapshot_calls),
            "窗口数一致": summary.get("窗口数") == (snapshot or {}).get("窗口数"),
            "调用数一致": (
                int(summary.get("影响调用数") or 0) == int((snapshot or {}).get("行数统计") or 0)
                == len(calls)
            ),
            "采用范围一致": (
                set(scope.get("window_keys", [])) == set((snapshot or {}).get("采用窗口键", []))
                and calls == snapshot_calls
            ),
            "汇总一致": bool(snapshot) and summary == snapshot_summary,
            "跨报告重复调用": overlapping,
            "跨报告无重复": not overlapping,
        }

    def archive_report(self, ref: Any) -> dict[str, Any]:
        report = self._resolve_report(ref)
        if report is None:
            return {"ok": False, "message": f"治理报告{ref}不存在"}
        if not isinstance(report.get("fault_scope"), dict):
            return {"ok": False, "message": "该报告不是采集故障分布取用报告"}

        scope = report["fault_scope"]
        already_archived = report.get("status") == "已归档" and report.get("archive", {}).get("状态") == "已完成"
        if already_archived:
            snapshot = self._snapshot_for_report(report)
            reconciliation = self._reconcile(report, snapshot)
            return {
                "ok": True,
                "reused": True,
                "duplicate_snapshot_created": False,
                "message": (
                    f"报告{report['报告编号']}已归档，重试核对："
                    f"等待中的更正已全部进入报告，快照{(snapshot or {}).get('版本编号')}未重复创建"
                ),
                "report": copy.deepcopy(report),
                "reconciliation": reconciliation,
            }

        report["archive_state"] = "归档中"
        attempted_at = _iso(_now())
        pending = [
            correction for correction in self.report_fault_corrections(report)
            if correction["状态"] == "待应用"
        ]
        applied_ids = set(scope.get("applied_correction_ids", []))
        applied_logs = {
            int(correction["来源日志编号"])
            for correction in self.report_fault_corrections(report)
            if int(correction["id"]) in applied_ids
        }
        # 中断重试：有些更正可能上一轮已经进了报告（日志已计入原依据），
        # 这里按“更正编号 + 来源日志编号”双重复核，只补真正还没进的。
        already_in = [
            correction for correction in pending
            if int(correction["id"]) in applied_ids
            or int(correction["来源日志编号"]) in applied_logs
        ]
        for correction in already_in:
            correction["状态"] = "已应用"
            correction["status"] = "已应用"
            correction["pending"] = False
            correction["应用时间"] = correction.get("应用时间") or attempted_at
            correction["去向"]["应用报告"] = True
            correction["去向"]["结果"] = "重试核对：更正已在报告中，未重复累计"
        freshly_applied = [correction for correction in pending if correction not in already_in]
        if freshly_applied:
            self._apply_corrections(report, freshly_applied, mark=True)
        elif not scope.get("summary"):
            self._apply_corrections(report, [], mark=False)

        fingerprint = self._fingerprint(report)
        summary = scope.get("summary", {})
        versions = store.rows(self.VERSION_MODULE)
        snapshot = self._snapshot_for_report(report)
        reused = snapshot is not None
        payload = {
            "报告编号": report.get("报告编号"),
            "数据来源": "fault_distribution",
            "行数统计": summary.get("影响调用数", 0),
            "版本标签": "fault_distribution_archive",
            "Schema摘要": "采集故障时间分布",
            "创建原因": f"治理报告{report.get('报告编号')}归档",
            "依赖版本": report.get("报告编号"),
            "创建时间": _now().date().isoformat(),
            "status": "可用",
            "pending": False,
            "abnormal": False,
            "窗口数": summary.get("窗口数", 0),
            "影响调用数": summary.get("影响调用数", 0),
            "采用窗口键": sorted(scope.get("window_keys", [])),
            "采用调用编号": sorted(scope.get("adopted_call_numbers", [])),
            "fault_scope_summary": copy.deepcopy(summary),
            "scope指纹": fingerprint,
            "更正编号": sorted(scope.get("applied_correction_ids", [])),
        }
        if snapshot is None:
            payload["id"] = self._next_id(versions)
            payload["版本编号"] = f"DATA-FAULT-{str(report.get('id')).zfill(4)}"
            versions.append(payload)
            snapshot = payload
        else:
            snapshot.update(payload)

        for correction in self.report_fault_corrections(report):
            if correction["状态"] == "已应用":
                versions_list = correction["去向"].setdefault("归档版本", [])
                if snapshot["版本编号"] not in versions_list:
                    versions_list.append(snapshot["版本编号"])
                correction["去向"]["结果"] = "已复算进报告并随报告归档"

        reconciliation = self._reconcile(report, snapshot)
        report.pop("archive_state", None)
        report["archive"] = {
            "状态": "已完成",
            "快照版本": snapshot["版本编号"],
            "scope指纹": fingerprint,
            "归档时间": attempted_at,
            "是否复用已有快照": reused,
            "本次补入更正": [correction["更正编号"] for correction in freshly_applied],
            "核对为已进报告的更正": [correction["更正编号"] for correction in already_in],
            "报告汇总": copy.deepcopy(summary),
            "对账": reconciliation,
        }
        for correction in self.report_fault_corrections(report):
            correction["去向"]["报告状态"] = "已归档"
        report["status"] = "已归档"
        report["pending"] = False
        report["abnormal"] = False
        return {
            "ok": True,
            "reused": reused,
            "duplicate_snapshot_created": False,
            "message": (
                f"报告{report['报告编号']}已归档，"
                f"{'复用' if reused else '创建'}快照{snapshot['版本编号']}，"
                f"补入{len(freshly_applied)}条更正，核对已进报告{len(already_in)}条"
            ),
            "report": copy.deepcopy(report),
            "snapshot": copy.deepcopy(snapshot),
            "reconciliation": reconciliation,
        }

    def consistency(self, ref: Any) -> dict[str, Any]:
        report = self._resolve_report(ref)
        if report is None:
            return {"ok": False, "message": f"治理报告{ref}不存在"}
        snapshot = self._snapshot_for_report(report)
        reconciliation = self._reconcile(report, snapshot)
        consistent = all([
            reconciliation["窗口数一致"],
            reconciliation["调用数一致"],
            reconciliation["采用范围一致"],
            reconciliation["汇总一致"],
            reconciliation["跨报告无重复"],
        ])
        return {
            "ok": True,
            "consistent": consistent,
            "report": copy.deepcopy(report),
            "snapshot": copy.deepcopy(snapshot),
            "reconciliation": reconciliation,
        }

    # ---------------------------------------------------------------- 更正/追踪
    def corrections(self, call_no: str | None = None) -> dict[str, Any]:
        rows = [
            copy.deepcopy(correction) for correction in self._corrections()
            if not call_no or correction.get("调用编号") == call_no
        ]
        return {
            "items": rows,
            "total": len(rows),
            "pending": sum(1 for row in rows if row.get("状态") == "待应用"),
            "applied": sum(1 for row in rows if row.get("状态") == "已应用"),
            "destination_rule": "每条更正列出所去报告、报告状态与归档版本；未去报告时说明只进入分布视图。",
        }

    def trace(self, call_no: str) -> dict[str, Any]:
        logs = sorted(
            [log for log in self._logs() if log.get("调用编号") == call_no],
            key=lambda row: (self._log_time(row) or datetime.max, int(row["id"])),
        )
        windows = self._build_windows(logs)
        keys = {window["窗口键"] for window in windows}
        corrections = [
            copy.deepcopy(correction) for correction in self._corrections()
            if correction.get("调用编号") == call_no
        ]
        reports = []
        for report in self._reports():
            scope = report["fault_scope"]
            if not (keys & set(scope.get("window_keys", []))):
                continue
            counted = 1 if call_no in scope.get("adopted_call_numbers", []) else 0
            snapshot = self._snapshot_for_report(report)
            reports.append({
                "报告编号": report.get("报告编号"),
                "报告状态": report.get("status"),
                "报告窗口数": scope.get("summary", {}).get("窗口数"),
                "报告影响调用数": scope.get("summary", {}).get("影响调用数"),
                "本调用计入次数": counted,
                "采用的开始时间": next(
                    (item.get("开始时间") for item in scope.get("window_snapshot", [])
                     if item.get("调用编号") == call_no), None),
                "采用的结束时间": next(
                    (item.get("结束时间") for item in scope.get("window_snapshot", [])
                     if item.get("调用编号") == call_no), None),
                "已应用更正编号": list(scope.get("applied_correction_ids", [])),
                "待应用更正编号": [
                    correction.get("更正编号") for correction in corrections
                    if correction.get("报告编号") == report.get("报告编号")
                    and correction.get("状态") == "待应用"
                ],
                "归档版本": (snapshot or {}).get("版本编号"),
                "归档计入次数": (1 if snapshot and call_no in snapshot.get("采用调用编号", []) else 0) if snapshot else 0,
            })
        report_counts = [row["本调用计入次数"] for row in reports]
        archive_counts = [row["归档计入次数"] for row in reports if row["归档版本"]]
        return {
            "调用编号": call_no,
            "logs": [copy.deepcopy(log) for log in logs],
            "logs_total": len(logs),
            "failure_logs": sum(1 for log in logs if _normalise_kind(log.get("事件类型")) == FAILURE),
            "recovery_logs": sum(1 for log in logs if _normalise_kind(log.get("事件类型")) == RECOVERY),
            "windows": [self._public_window(window) for window in windows],
            "window_total": len(windows),
            "reports": reports,
            "corrections": corrections,
            "counting": {
                "分布中窗口数": len(windows),
                "分布中调用数": 1 if windows else 0,
                "报告计入次数": report_counts,
                "归档计入次数": archive_counts,
                "同一调用在报告中重复累计": sum(report_counts) > 1,
                "同一调用在归档中重复累计": sum(archive_counts) > 1,
                "恰好一次": sum(report_counts) <= 1 and sum(archive_counts) <= 1,
            },
        }

    def overview(self) -> dict[str, Any]:
        logs = self._logs()
        windows = self._build_windows(logs)
        start, end = self._resolve_period({}, logs, windows)
        summary = self._summarise(windows, start, end)
        return {
            "logs_total": len(logs),
            "windows_total": len(windows),
            "ongoing_total": sum(1 for window in windows if window.get("_end") is None),
            "corrections_total": len(self._corrections()),
            "corrections_pending": sum(
                1 for correction in self._corrections() if correction.get("状态") == "待应用"),
            "reports_total": len(self._reports()),
            "reports_archived": sum(1 for report in self._reports() if report.get("status") == "已归档"),
            "today_summary": summary,
        }

    def list_logs(self, page: int = 1, size: int = 100) -> dict[str, Any]:
        rows = sorted(
            self._logs(), key=lambda row: (self._log_time(row) or datetime.max, int(row["id"])))
        total = len(rows)
        start = max(0, (page - 1) * size)
        return {"items": copy.deepcopy(rows[start:start + size]), "total": total, "page": page, "size": size}

    def list_reports(self) -> dict[str, Any]:
        rows = [copy.deepcopy(report) for report in self._reports()]
        for row in rows:
            row["对账"] = self._reconcile(
                next(report for report in self._reports() if report.get("id") == row["id"]),
                self._snapshot_for_report(row),
            )
        return {"items": rows, "total": len(rows)}
