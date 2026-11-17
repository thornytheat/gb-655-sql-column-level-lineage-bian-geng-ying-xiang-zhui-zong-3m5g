"""迁移单流转：签发、签收、更正、补偿确认。

状态机：issued → signed →（更正）voided →（补偿确认）settled。
- 更正另发新单并把原单作废，旧内容与签收凭据留在原单可回读。
- 已签收的单被更正时登记待补偿，补偿确认后原单才结清。
- 单号由批次号加修订号推导，重试拿到同一个单号；幂等键让重试不重复发单或补偿。
"""

from __future__ import annotations

import re
import sqlite3

from app import config, errors, store
from app.manifest import sha256_bytes
from app.modules.m8_orders import content as content_mod

ORDER_STATUS_LABELS = {
    "issued": "已签发",
    "signed": "已签收",
    "voided": "已作废",
    "settled": "已结清",
}
COMP_STATUS_LABELS = {"pending": "待补偿", "confirmed": "已确认"}

ACTIVE_STATUSES = ("issued", "signed")
TERMINAL_STATUSES = ("voided", "settled")

KEY_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,128}$")

MAX_AMENDMENTS = 20


# ---------- 入参校验 ----------

def _clean_key(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not KEY_RE.match(value.strip()):
        raise errors.BadInput("idempotency_key 只允许字母数字与 _ . : -，最长 128",
                             detail={"field": "idempotency_key"})
    return value.strip()


def _clean_text(value, field: str, *, required: bool, limit: int) -> str | None:
    if value is None:
        if required:
            raise errors.BadInput(f"{field} 不能为空", detail={"field": field})
        return None
    if not isinstance(value, str) or not value.strip():
        if required:
            raise errors.BadInput(f"{field} 不能为空", detail={"field": field})
        return None
    text = value.strip()
    if len(text) > limit:
        raise errors.BadInput(f"{field} 最长 {limit} 字",
                             detail={"field": field, "limit": limit})
    return text


def _clean_revision(value) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise errors.BadInput("expected_revision 必须是整数",
                             detail={"value": repr(value)})
    if value < 1:
        raise errors.BadInput("expected_revision 不能小于 1", detail={"value": value})
    return value


def _clean_amendments(value) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_AMENDMENTS:
        raise errors.BadInput(f"amendments 必须是最多 {MAX_AMENDMENTS} 条的列表",
                             detail={"field": "amendments"})
    out = []
    for item in value:
        if not isinstance(item, dict):
            raise errors.BadInput("每条更正条款必须是对象", detail={"field": "amendments"})
        unknown = set(item) - {"clause", "text"}
        if unknown:
            raise errors.BadInput("更正条款出现未知字段",
                                 detail={"keys": sorted(unknown)})
        text = _clean_text(item.get("text"), "text", required=True, limit=500)
        clause = _clean_text(item.get("clause"), "clause", required=False, limit=32)
        out.append({"clause": clause, "text": text})
    return out


# ---------- 展示 ----------

def describe_comp(comp: dict | None) -> dict | None:
    if comp is None:
        return None
    return {
        "comp_id": comp["comp_id"],
        "order_id": comp["order_id"],
        "new_order_id": comp["new_order_id"],
        "status": comp["status"],
        "status_label": COMP_STATUS_LABELS.get(comp["status"], comp["status"]),
        "created_at": comp["created_at"],
        "confirmed_at": comp["confirmed_at"],
    }


def describe(order: dict) -> dict:
    signoff = store.signoff_for_order(order["order_id"])
    comp = store.compensation_for_order(order["order_id"])
    return {
        "order_id": order["order_id"],
        "run_id": order["run_id"],
        "revision": order["revision"],
        "supersedes": order["supersedes"],
        "status": order["status"],
        "status_label": ORDER_STATUS_LABELS.get(order["status"], order["status"]),
        "content_sha256": order["content_sha256"],
        "content_bytes": len(order["content_text"].encode("utf-8")),
        "reason": order["reason"],
        "amendments": order["amendments"],
        "issued_at": order["issued_at"],
        "voided_at": order["voided_at"],
        "void_reason": order["void_reason"],
        "settled_at": order["settled_at"],
        "signoff": signoff,
        "compensation": describe_comp(comp),
    }


def _get_order_or_404(order_id: str) -> dict:
    order = store.order_by_id(order_id)
    if order is None:
        raise errors.NotFound("迁移单不存在", detail={"order_id": order_id})
    return order


# ---------- 签发 ----------

def issue(run_id: str, *, note=None, idempotency_key=None) -> dict:
    key = _clean_key(idempotency_key)
    note = _clean_text(note, "note", required=False, limit=200)
    run = store.run_by_id(run_id)
    if run is None:
        raise errors.NotFound("批次不存在，先跑一次再签发",
                             detail={"run_id": run_id})
    if key:
        prior = store.order_by_idempotency(key)
        if prior is not None:
            if prior["run_id"] != run_id:
                raise errors.Conflict("幂等键已被其他签发占用",
                                     detail={"key": key, "order_id": prior["order_id"]})
            return {"order": describe(prior), "replayed": True}
    existing = store.latest_order_for_run(run_id)
    if existing is not None:
        raise errors.Conflict(
            "该批次已有迁移单，条款有误请走更正",
            detail={"order_id": existing["order_id"], "status": existing["status"]},
        )
    issued_at = config.now_utc().isoformat()
    text, sha = content_mod.freeze(run, revision=1, supersedes=None, reason=None,
                                   amendments=[], issued_at=issued_at)
    order = {
        "order_id": f"{run_id}-r1",
        "run_id": run_id,
        "revision": 1,
        "supersedes": None,
        "status": "issued",
        "content_text": text,
        "content_sha256": sha,
        "reason": note,
        "amendments": [],
        "idempotency_key": key,
        "issued_at": issued_at,
    }
    try:
        store.save_order(order)
    except sqlite3.IntegrityError:
        current = store.latest_order_for_run(run_id)
        raise errors.Conflict(
            "签发与其他请求冲突，按单号回读即可",
            detail={"order_id": (current or {}).get("order_id")},
        )
    return {"order": describe(store.order_by_id(order["order_id"])), "replayed": False}


# ---------- 签收 ----------

def signoff(order_id: str, *, signer=None, idempotency_key=None) -> dict:
    key = _clean_key(idempotency_key)
    signer = _clean_text(signer, "signer", required=True, limit=64)
    if key:
        prior = store.signoff_by_idempotency(key)
        if prior is not None:
            if prior["order_id"] != order_id:
                raise errors.Conflict("幂等键已被其他签收占用",
                                     detail={"key": key, "order_id": prior["order_id"]})
            return {"order": describe(_get_order_or_404(order_id)),
                    "signoff": prior, "replayed": True}
    order = _get_order_or_404(order_id)
    receipt = "rcpt-" + sha256_bytes(
        f"{order_id}|{signer}|{order['content_sha256']}".encode("utf-8")
    )[:16]
    record = {
        "order_id": order_id,
        "signer": signer,
        "signed_at": config.now_utc().isoformat(),
        "receipt": receipt,
        "content_sha256": order["content_sha256"],
        "idempotency_key": key,
    }
    try:
        outcome = store.signoff_tx(signoff=record)
    except sqlite3.IntegrityError:
        outcome = {"outcome": "already", "signoff": store.signoff_for_order(order_id)}
    if outcome["outcome"] == "missing":
        raise errors.NotFound("迁移单不存在", detail={"order_id": order_id})
    if outcome["outcome"] == "voided":
        raise errors.OrderVoided(
            "单已作废，迟到签收不予受理",
            detail={"order_id": order_id, "status": outcome["status"]},
        )
    return {
        "order": describe(_get_order_or_404(order_id)),
        "signoff": outcome["signoff"] or store.signoff_for_order(order_id),
        "replayed": outcome["outcome"] == "already",
    }


# ---------- 更正 ----------

def correct(order_id: str, *, expected_revision=None, reason=None, amendments=None,
            idempotency_key=None) -> dict:
    key = _clean_key(idempotency_key)
    reason = _clean_text(reason, "reason", required=True, limit=200)
    expected = _clean_revision(expected_revision)
    items = _clean_amendments(amendments)
    if key:
        prior = store.order_by_idempotency(key)
        if prior is not None:
            if prior["supersedes"] != order_id:
                raise errors.Conflict("幂等键已被其他更正占用",
                                     detail={"key": key, "order_id": prior["order_id"]})
            return _correct_result(prior, replayed=True)
    old = _get_order_or_404(order_id)
    if old["status"] in TERMINAL_STATUSES:
        raise errors.OrderVoided(
            f"原单已{ORDER_STATUS_LABELS[old['status']]}，更正请对在途单",
            detail={"order_id": order_id, "status": old["status"]},
        )
    if expected != old["revision"]:
        raise errors.StaleVersion(
            f"基于第 {expected} 版的更正已落后，当前在途是第 {old['revision']} 版",
            detail={"expected_revision": expected, "current_revision": old["revision"],
                    "order_id": order_id, "reason": "REVISION_STALE"},
        )
    run = store.run_by_id(old["run_id"])
    if run is None:
        raise errors.NotFound("批次不存在", detail={"run_id": old["run_id"]})
    issued_at = config.now_utc().isoformat()
    new_revision = old["revision"] + 1
    text, sha = content_mod.freeze(run, revision=new_revision,
                                   supersedes=old["order_id"], reason=reason,
                                   amendments=items, issued_at=issued_at)
    new_order = {
        "order_id": f"{old['run_id']}-r{new_revision}",
        "run_id": old["run_id"],
        "revision": new_revision,
        "supersedes": old["order_id"],
        "status": "issued",
        "content_text": text,
        "content_sha256": sha,
        "reason": reason,
        "amendments": items,
        "idempotency_key": key,
        "issued_at": issued_at,
    }
    compensation = None
    if old["status"] == "signed":
        compensation = {
            "comp_id": f"cmp-{old['order_id']}",
            "order_id": old["order_id"],
            "new_order_id": new_order["order_id"],
            "status": "pending",
            "idempotency_key": None,
            "created_at": issued_at,
            "confirmed_at": None,
        }
    try:
        outcome = store.correct_order_tx(
            old_order_id=order_id, expected_revision=expected, voided_at=issued_at,
            void_reason=reason, new_order=new_order, compensation=compensation,
        )
    except sqlite3.IntegrityError:
        raise errors.Conflict("更正与其他请求冲突，回读最新单后重试",
                             detail={"order_id": order_id})
    if not outcome["ok"]:
        current = outcome["current"]
        if current is None:
            raise errors.NotFound("迁移单不存在", detail={"order_id": order_id})
        if current["status"] in TERMINAL_STATUSES:
            raise errors.OrderVoided(
                f"原单已{ORDER_STATUS_LABELS[current['status']]}，更正请对在途单",
                detail={"order_id": order_id, "status": current["status"]},
            )
        raise errors.StaleVersion(
            f"基于第 {expected} 版的更正已落后，当前在途是第 {current['revision']} 版",
            detail={"expected_revision": expected,
                    "current_revision": current["revision"],
                    "order_id": order_id, "reason": "REVISION_STALE"},
        )
    return _correct_result(new_order, replayed=False)


def _correct_result(new_order: dict, *, replayed: bool) -> dict:
    old = store.order_by_id(new_order["supersedes"])
    return {
        "order": describe(store.order_by_id(new_order["order_id"])),
        "voided": describe(old) if old else None,
        "compensation": describe_comp(store.compensation_for_order(new_order["supersedes"])),
        "replayed": replayed,
    }


# ---------- 补偿确认 ----------

def confirm(comp_id: str, *, idempotency_key=None) -> dict:
    key = _clean_key(idempotency_key)
    if key:
        prior = store.compensation_by_idempotency(key)
        if prior is not None:
            if prior["comp_id"] != comp_id:
                raise errors.Conflict("幂等键已被其他补偿占用",
                                     detail={"key": key, "comp_id": prior["comp_id"]})
            return _confirm_result(prior["comp_id"], replayed=True)
    try:
        outcome = store.confirm_compensation_tx(comp_id, config.now_utc().isoformat(), key)
    except sqlite3.IntegrityError:
        raise errors.Conflict("幂等键已被其他补偿占用",
                             detail={"key": key, "comp_id": comp_id})
    if outcome["outcome"] == "missing":
        raise errors.NotFound("补偿登记不存在", detail={"comp_id": comp_id})
    return _confirm_result(comp_id, replayed=outcome["outcome"] == "already")


def _confirm_result(comp_id: str, *, replayed: bool) -> dict:
    comp = store.compensation_by_id(comp_id)
    return {
        "compensation": describe_comp(comp),
        "order": describe(_get_order_or_404(comp["order_id"])),
        "replayed": replayed,
    }


# ---------- 回读与核对 ----------

def readback(order_id: str) -> dict:
    return describe(_get_order_or_404(order_id))


def frozen_content(order_id: str) -> tuple[str, str]:
    order = _get_order_or_404(order_id)
    return order["content_text"], order["content_sha256"]


def chain(run_id: str) -> dict:
    if store.run_by_id(run_id) is None:
        raise errors.NotFound("批次不存在", detail={"run_id": run_id})
    orders = store.orders_for_run(run_id)
    current = next((o for o in orders if o["status"] in ACTIVE_STATUSES), None)
    problems = _chain_problems(orders)
    return {
        "run_id": run_id,
        "current_order_id": current["order_id"] if current else None,
        "orders": [describe(o) for o in orders],
        "consistent": not problems,
        "problems": problems,
    }


def _chain_problems(orders: list[dict]) -> list[dict]:
    problems = []

    def add(code: str, detail: str, order_id: str | None = None):
        problems.append({"code": code, "detail": detail, "order_id": order_id})

    active = [o for o in orders if o["status"] in ACTIVE_STATUSES]
    if len(active) > 1:
        add("MULTIPLE_ACTIVE", f"在途单 {len(active)} 张，应为 1 张")
    for index, order in enumerate(orders):
        oid = order["order_id"]
        if order["revision"] != index + 1:
            add("REVISION_GAP", f"修订号 {order['revision']} 与链上位置 {index + 1} 不符", oid)
        if index == 0:
            if order["supersedes"]:
                add("FIRST_HAS_SUPERSEDES", "首单不应有前序单号", oid)
        elif order["supersedes"] != orders[index - 1]["order_id"]:
            add("CHAIN_BROKEN", "前序单号对不上上一张", oid)
        signoff = store.signoff_for_order(oid)
        comp = store.compensation_for_order(oid)
        if order["status"] == "signed" and signoff is None:
            add("SIGNED_WITHOUT_RECEIPT", "已签收但缺签收凭据", oid)
        if order["status"] in TERMINAL_STATUSES and signoff is not None and comp is None:
            add("VOIDED_SIGNED_WITHOUT_COMP", "已签收的单作废后缺待补偿登记", oid)
        if order["status"] == "settled" and (comp is None or comp["status"] != "confirmed"):
            add("SETTLED_WITHOUT_CONFIRM", "已结清但补偿未确认", oid)
        if comp is not None and comp["status"] == "confirmed" and order["status"] != "settled":
            add("CONFIRMED_NOT_SETTLED", "补偿已确认但单未结清", oid)
        if (comp is not None and index + 1 < len(orders)
                and comp["new_order_id"] != orders[index + 1]["order_id"]):
            add("COMP_TARGET_MISMATCH", "补偿指向的新单与链上下一张不符", oid)
    return problems


def pending() -> dict:
    """未完成处置：中断恢复后从这里接着办，重试不会重复发单或补偿。"""
    unsigned = [describe(o) for o in store.orders_unsigned()]
    comps = [describe_comp(c) for c in store.compensations_pending()]
    return {
        "unsigned_orders": unsigned,
        "pending_compensations": comps,
        "counts": {"unsigned": len(unsigned), "compensations": len(comps)},
    }
