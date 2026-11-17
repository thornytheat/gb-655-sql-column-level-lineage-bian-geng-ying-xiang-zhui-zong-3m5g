"""M8 入口：迁移单签发、签收、更正、补偿确认与回读核对。"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response

from app import errors
from app.modules.m8_orders import service

router = APIRouter()

ISSUE_KEYS = {"note", "idempotency_key"}
SIGNOFF_KEYS = {"signer", "idempotency_key"}
CORRECT_KEYS = {"expected_revision", "reason", "amendments", "idempotency_key"}
CONFIRM_KEYS = {"idempotency_key"}


def _body(payload: dict | None, allowed: set[str]) -> dict:
    body = payload or {}
    if not isinstance(body, dict):
        raise errors.BadInput("请求体必须是对象")
    unknown = set(body) - allowed
    if unknown:
        raise errors.BadInput("出现未知参数", detail={"keys": sorted(unknown)})
    return body


@router.post("/api/runs/{run_id}/orders")
def issue_order(run_id: str, payload: dict | None = None):
    body = _body(payload, ISSUE_KEYS)
    return service.issue(run_id, note=body.get("note"),
                         idempotency_key=body.get("idempotency_key"))


@router.post("/api/orders/{order_id}/signoff")
def signoff_order(order_id: str, payload: dict | None = None):
    body = _body(payload, SIGNOFF_KEYS)
    return service.signoff(order_id, signer=body.get("signer"),
                           idempotency_key=body.get("idempotency_key"))


@router.post("/api/orders/{order_id}/correct")
def correct_order(order_id: str, payload: dict | None = None):
    body = _body(payload, CORRECT_KEYS)
    return service.correct(order_id, expected_revision=body.get("expected_revision"),
                           reason=body.get("reason"), amendments=body.get("amendments"),
                           idempotency_key=body.get("idempotency_key"))


@router.post("/api/compensations/{comp_id}/confirm")
def confirm_compensation(comp_id: str, payload: dict | None = None):
    body = _body(payload, CONFIRM_KEYS)
    return service.confirm(comp_id, idempotency_key=body.get("idempotency_key"))


@router.get("/api/orders/pending")
def pending_orders():
    """未完成处置清单：中断恢复后从这里接着办。"""
    return service.pending()


@router.get("/api/orders/{order_id}")
def read_order(order_id: str):
    return service.readback(order_id)


@router.get("/api/orders/{order_id}/content")
def order_content(order_id: str):
    text, sha = service.frozen_content(order_id)
    return Response(
        content=text,
        media_type="text/markdown; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{order_id}.md"',
            "X-Content-SHA256": sha,
        },
    )


@router.get("/api/runs/{run_id}/orders")
def run_orders(run_id: str):
    """核对：同一批次的新旧单链、签收与补偿状态。"""
    return service.chain(run_id)
