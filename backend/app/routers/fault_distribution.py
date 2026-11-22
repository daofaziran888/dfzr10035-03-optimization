"""采集故障时间分布接口：日志录入、分布视图、报告复算/归档与调用追踪。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.fault_distribution import FaultDistributionService

router = APIRouter(prefix="/api/fault-distribution", tags=["采集故障时间分布"])
service = FaultDistributionService()


@router.get("/overview")
def overview() -> dict[str, Any]:
    return service.overview()


@router.get("")
def distribution(
    period_start: str | None = Query(default=None),
    period_end: str | None = Query(default=None),
    endpoint: str | None = Query(default=None),
    bucket_minutes: int = Query(default=15, ge=1, le=1440),
) -> dict[str, Any]:
    return service.distribution(period_start, period_end, endpoint, bucket_minutes)


@router.get("/windows")
def windows(
    period_start: str | None = Query(default=None),
    period_end: str | None = Query(default=None),
    endpoint: str | None = Query(default=None),
    include_ongoing: bool = Query(default=True),
) -> dict[str, Any]:
    return service.windows(period_start, period_end, endpoint, include_ongoing)


@router.get("/logs")
def list_logs(page: int = Query(default=1, ge=1), size: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    return service.list_logs(page, size)


@router.post("/logs")
def ingest_log(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    values = payload.get("values") if isinstance(payload.get("values"), dict) else payload
    result = service.ingest(values or {})
    if not result.get("ok"):
        raise HTTPException(status_code=422, detail=result.get("message"))
    return result


@router.get("/corrections")
def corrections(call_no: str | None = Query(default=None)) -> dict[str, Any]:
    return service.corrections(call_no)


@router.get("/reports")
def list_reports() -> dict[str, Any]:
    return service.list_reports()


@router.post("/reports")
def generate_report(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    values = payload.get("values") if isinstance(payload.get("values"), dict) else payload
    result = service.generate_report(values or {})
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result)
    return result


@router.get("/reports/{ref}")
def get_report(ref: str) -> dict[str, Any]:
    report = service._resolve_report(ref)
    if report is None:
        raise HTTPException(status_code=404, detail=f"治理报告{ref}不存在")
    return report


@router.get("/reports/{ref}/consistency")
def report_consistency(ref: str) -> dict[str, Any]:
    result = service.consistency(ref)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message"))
    return result


@router.post("/reports/{ref}/recalculate")
def recalculate(ref: str) -> dict[str, Any]:
    result = service.recalculate(ref)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result)
    return result


@router.post("/reports/{ref}/archive")
def archive(ref: str) -> dict[str, Any]:
    result = service.archive_report(ref)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result)
    return result


@router.get("/trace/{call_no}")
def trace(call_no: str) -> dict[str, Any]:
    return service.trace(call_no)
