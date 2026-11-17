"""M8 迁移单流转：签发、签收、更正、补偿、并发、恢复与核对。"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import errors, store
from app.modules.m7_migration import service as migration
from app.modules.m8_orders import service as orders


@pytest.fixture()
def run_id(client) -> str:
    return store.latest_run()["run_id"]


def _issue(client, rid, **body):
    return client.post(f"/api/runs/{rid}/orders", json=body or {})


def _sign(client, oid, **body):
    body.setdefault("signer", "下游-报表组")
    return client.post(f"/api/orders/{oid}/signoff", json=body)


def _correct(client, oid, **body):
    body.setdefault("expected_revision", 1)
    body.setdefault("reason", "条款 3.2 类型写法有误")
    return client.post(f"/api/orders/{oid}/correct", json=body)


def _signed_order(client, rid) -> str:
    oid = _issue(client, rid).json()["order"]["order_id"]
    assert _sign(client, oid).status_code == 200
    return oid


# ---------- 签发 ----------

def test_issue_freezes_content(client, run_id):
    resp = _issue(client, run_id, note="首发")
    assert resp.status_code == 200
    order = resp.json()["order"]
    assert order["order_id"] == f"{run_id}-r1"
    assert order["revision"] == 1
    assert order["status"] == "issued"
    assert order["status_label"] == "已签发"
    assert len(order["content_sha256"]) == 64

    run = store.latest_run()
    assert order["content_sha256"] == orders.content_mod.freeze(
        run, revision=1, supersedes=None, reason=None, amendments=[],
        issued_at=order["issued_at"])[1]


def test_issue_rejects_second_order_for_same_run(client, run_id):
    assert _issue(client, run_id).status_code == 200
    again = _issue(client, run_id)
    assert again.status_code == 409
    assert again.json()["error"] == "CONFLICT"
    assert again.json()["detail"]["order_id"] == f"{run_id}-r1"


def test_issue_replay_same_key_returns_same_order(client, run_id):
    first = _issue(client, run_id, idempotency_key="issue-001")
    second = _issue(client, run_id, idempotency_key="issue-001")
    assert first.status_code == second.status_code == 200
    assert second.json()["replayed"] is True
    assert second.json()["order"]["order_id"] == first.json()["order"]["order_id"]
    assert len(store.orders_for_run(run_id)) == 1


def test_issue_key_reused_by_other_run_conflicts(client, run_id):
    assert _issue(client, run_id, idempotency_key="k-dup").status_code == 200
    other = _issue(client, "fm-nonexistent", idempotency_key="k-dup")
    assert other.status_code == 404  # 批次先 404；键不串用由同批次的冲突覆盖


def test_issue_unknown_run_404(client):
    assert _issue(client, "fm-000000000000").status_code == 404


# ---------- 签收 ----------

def test_signoff_happy_path_and_idempotent(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    first = _sign(client, oid)
    assert first.status_code == 200
    body = first.json()
    assert body["order"]["status"] == "signed"
    assert body["signoff"]["receipt"].startswith("rcpt-")
    assert body["replayed"] is False

    retry = _sign(client, oid)
    assert retry.status_code == 200
    assert retry.json()["replayed"] is True
    assert retry.json()["signoff"]["receipt"] == body["signoff"]["receipt"]


def test_signoff_requires_signer(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    resp = client.post(f"/api/orders/{oid}/signoff", json={})
    assert resp.status_code == 400
    assert resp.json()["detail"]["field"] == "signer"


def test_signoff_unknown_order_404(client):
    assert _sign(client, "fm-x-r9").status_code == 404


# ---------- 更正 ----------

def test_correct_voids_original_and_keeps_content(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    old_content = client.get(f"/api/orders/{oid}/content").text

    resp = _correct(client, oid, amendments=[{"clause": "3.2", "text": "int 应为 bigint"}])
    assert resp.status_code == 200
    body = resp.json()
    new_order = body["order"]
    assert new_order["order_id"] == f"{run_id}-r2"
    assert new_order["supersedes"] == oid
    assert body["voided"]["status"] == "voided"
    assert body["voided"]["void_reason"]
    assert body["compensation"] is None  # 未签收不登记补偿

    # 旧内容原样保留，新内容带更正说明
    assert client.get(f"/api/orders/{oid}/content").text == old_content
    new_text = client.get(f"/api/orders/{new_order['order_id']}/content").text
    assert "更正说明" in new_text and "int 应为 bigint" in new_text
    assert oid in new_text


def test_late_signoff_on_voided_order_rejected(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    assert _correct(client, oid).status_code == 200

    late = _sign(client, oid)
    assert late.status_code == 409
    assert late.json()["error"] == "ORDER_VOIDED"


def test_correct_after_signoff_registers_pending_compensation(client, run_id):
    oid = _signed_order(client, run_id)
    resp = _correct(client, oid)
    assert resp.status_code == 200
    body = resp.json()
    comp = body["compensation"]
    assert comp["status"] == "pending"
    assert comp["status_label"] == "待补偿"
    assert comp["order_id"] == oid
    assert comp["new_order_id"] == body["order"]["order_id"]
    # 确认前旧单只是作废，没有结清
    old = client.get(f"/api/orders/{oid}").json()
    assert old["status"] == "voided"
    assert old["settled_at"] is None
    assert old["signoff"]["receipt"].startswith("rcpt-")  # 签收凭据保留


def test_settle_only_after_compensation_confirmed(client, run_id):
    oid = _signed_order(client, run_id)
    comp_id = _correct(client, oid).json()["compensation"]["comp_id"]

    done = client.post(f"/api/compensations/{comp_id}/confirm", json={})
    assert done.status_code == 200
    assert done.json()["compensation"]["status"] == "confirmed"
    assert done.json()["order"]["status"] == "settled"
    assert done.json()["order"]["settled_at"]

    again = client.post(f"/api/compensations/{comp_id}/confirm", json={})
    assert again.status_code == 200
    assert again.json()["replayed"] is True
    chain = client.get(f"/api/runs/{run_id}/orders").json()
    assert sum(1 for o in chain["orders"] if o["status"] == "settled") == 1


def test_confirm_unknown_compensation_404(client):
    assert client.post("/api/compensations/cmp-nope/confirm", json={}).status_code == 404


def test_signoff_on_settled_order_rejected(client, run_id):
    oid = _signed_order(client, run_id)
    comp_id = _correct(client, oid).json()["compensation"]["comp_id"]
    client.post(f"/api/compensations/{comp_id}/confirm", json={})
    resp = _sign(client, oid)
    assert resp.status_code == 409
    assert resp.json()["error"] == "ORDER_VOIDED"


# ---------- 并发与陈旧 ----------

def test_stale_revision_rejected(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    assert _correct(client, oid).status_code == 200  # r1 作废，r2 在途
    stale = client.post(f"/api/orders/{run_id}-r2/correct",
                        json={"expected_revision": 1, "reason": "旧版请求"})
    assert stale.status_code == 409
    assert stale.json()["error"] == "VERSION_STALE"
    assert stale.json()["detail"]["current_revision"] == 2


def test_correct_voided_order_rejected(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    assert _correct(client, oid).status_code == 200
    again = _correct(client, oid)
    assert again.status_code == 409
    assert again.json()["error"] == "ORDER_VOIDED"


def test_concurrent_correct_single_winner(client, run_id):
    oid = _signed_order(client, run_id)

    def attempt(tag):
        try:
            result = orders.correct(oid, expected_revision=1, reason=f"并发更正 {tag}")
            return ("ok", result["order"]["order_id"])
        except errors.FieldmapError as exc:
            return ("rejected", exc.code)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("甲", "乙")))
    outcomes = sorted(kind for kind, _ in results)
    assert outcomes == ["ok", "rejected"]

    chain = orders.chain(run_id)
    assert [o["order_id"] for o in chain["orders"]] == [f"{run_id}-r1", f"{run_id}-r2"]
    assert chain["consistent"] is True
    assert len(store.compensations_pending()) == 1  # 补偿也只登记一次


# ---------- 重试与中断恢复 ----------

def test_retry_same_key_no_duplicate_order_or_compensation(client, run_id):
    oid = _signed_order(client, run_id)
    first = _correct(client, oid, idempotency_key="fix-001")
    assert first.status_code == 200
    retry = _correct(client, oid, idempotency_key="fix-001")
    assert retry.status_code == 200
    assert retry.json()["replayed"] is True
    assert retry.json()["order"]["order_id"] == first.json()["order"]["order_id"]

    assert len(store.orders_for_run(run_id)) == 2
    assert len(store.compensations_pending()) == 1

    comp_id = first.json()["compensation"]["comp_id"]
    ok = client.post(f"/api/compensations/{comp_id}/confirm",
                     json={"idempotency_key": "pay-001"})
    assert ok.status_code == 200
    again = client.post(f"/api/compensations/{comp_id}/confirm",
                        json={"idempotency_key": "pay-001"})
    assert again.status_code == 200
    assert again.json()["replayed"] is True
    assert store.compensation_by_id(comp_id)["status"] == "confirmed"


def test_interrupted_correct_rolls_back(client, run_id, monkeypatch):
    oid = _signed_order(client, run_id)

    def boom(conn, comp):
        raise sqlite3.OperationalError("模拟中断：写补偿时掉线")

    original = store._insert_compensation
    monkeypatch.setattr(store, "_insert_compensation", boom)
    with pytest.raises(sqlite3.OperationalError):
        _correct(client, oid)

    # 中断不留半成品：旧单仍在途签收态，没有新单也没有补偿
    old = store.order_by_id(oid)
    assert old["status"] == "signed"
    assert len(store.orders_for_run(run_id)) == 1
    assert store.compensations_pending() == []

    monkeypatch.setattr(store, "_insert_compensation", original)
    resp = _correct(client, oid)
    assert resp.status_code == 200
    assert resp.json()["compensation"]["status"] == "pending"


def test_pending_lists_unfinished_and_survives_restart(client, run_id, tmp_path, monkeypatch):
    oid = _signed_order(client, run_id)
    assert _correct(client, oid).status_code == 200
    # r2 未签收 + r1 待补偿，都该在待办里
    before = client.get("/api/orders/pending").json()
    assert before["counts"] == {"unsigned": 1, "compensations": 1}
    assert before["unsigned_orders"][0]["order_id"] == f"{run_id}-r2"

    # 模拟重启：同一库再起一个应用实例，待办仍在，可接着办
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as again:
        resumed = again.get("/api/orders/pending").json()
        assert resumed["counts"] == {"unsigned": 1, "compensations": 1}
        comp_id = resumed["pending_compensations"][0]["comp_id"]
        assert again.post(f"/api/compensations/{comp_id}/confirm", json={}).status_code == 200
        assert again.get("/api/orders/pending").json()["counts"]["compensations"] == 0


# ---------- 回读、下载与核对 ----------

def test_readback_and_download_frozen_content(client, run_id):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    got = client.get(f"/api/orders/{oid}")
    assert got.status_code == 200
    assert got.json()["content_bytes"] > 0

    dl = client.get(f"/api/orders/{oid}/content")
    assert dl.status_code == 200
    assert dl.headers["content-type"].startswith("text/markdown")
    assert "attachment" in dl.headers["content-disposition"]
    assert dl.headers["x-content-sha256"] == got.json()["content_sha256"]
    assert dl.text.startswith(f"# 迁移单 {run_id}")
    # 冻结：再读一字不差
    assert client.get(f"/api/orders/{oid}/content").text == dl.text


def test_chain_reconcile_full_lifecycle(client, run_id):
    oid = _signed_order(client, run_id)
    comp_id = _correct(client, oid).json()["compensation"]["comp_id"]
    client.post(f"/api/compensations/{comp_id}/confirm", json={})
    # 新单签收后再更正一次，链更长
    assert _sign(client, f"{run_id}-r2").status_code == 200
    third = client.post(f"/api/orders/{run_id}-r2/correct",
                        json={"expected_revision": 2, "reason": "第二次更正"})
    assert third.status_code == 200

    chain = client.get(f"/api/runs/{run_id}/orders").json()
    assert chain["consistent"] is True
    assert chain["problems"] == []
    assert chain["current_order_id"] == f"{run_id}-r3"
    statuses = [o["status"] for o in chain["orders"]]
    assert statuses == ["settled", "voided", "issued"]
    assert chain["orders"][0]["compensation"]["status"] == "confirmed"
    assert chain["orders"][1]["compensation"]["status"] == "pending"
    assert chain["orders"][0]["signoff"]["receipt"]


def test_chain_unknown_run_404(client):
    assert client.get("/api/runs/fm-000000000000/orders").status_code == 404


def test_chain_empty_when_no_orders(client, run_id):
    chain = client.get(f"/api/runs/{run_id}/orders").json()
    assert chain["orders"] == []
    assert chain["consistent"] is True
    assert chain["current_order_id"] is None


# ---------- 边界与校验 ----------

def test_analysis_results_untouched_by_lifecycle(client, run_id):
    before = store.run_by_id(run_id)
    before_csv = migration.fields_csv_text(before)

    oid = _signed_order(client, run_id)
    comp_id = _correct(client, oid).json()["compensation"]["comp_id"]
    client.post(f"/api/compensations/{comp_id}/confirm", json={})

    after = store.run_by_id(run_id)
    assert after == before
    assert migration.fields_csv_text(after) == before_csv
    assert store.count_runs() == 1


@pytest.mark.parametrize("body", [
    {"expected_revision": "1", "reason": "x"},
    {"expected_revision": 0, "reason": "x"},
    {"expected_revision": True, "reason": "x"},
    {"expected_revision": 1},
    {"expected_revision": 1, "reason": "  "},
    {"expected_revision": 1, "reason": "x", "amendments": "not-a-list"},
    {"expected_revision": 1, "reason": "x", "amendments": [{"text": ""}]},
    {"expected_revision": 1, "reason": "x", "amendments": [{"text": "y", "extra": 1}]},
    {"expected_revision": 1, "reason": "x", "idempotency_key": "bad key!"},
    {"expected_revision": 1, "reason": "x", "unknown_field": 1},
])
def test_correct_validation_rejected(client, run_id, body):
    oid = _issue(client, run_id).json()["order"]["order_id"]
    resp = client.post(f"/api/orders/{oid}/correct", json=body)
    assert resp.status_code == 400, body


def test_old_order_keeps_content_and_receipt_after_void(client, run_id):
    oid = _signed_order(client, run_id)
    receipt = client.get(f"/api/orders/{oid}").json()["signoff"]["receipt"]
    content_before = client.get(f"/api/orders/{oid}/content").text

    assert _correct(client, oid).status_code == 200

    old = client.get(f"/api/orders/{oid}").json()
    assert old["status"] == "voided"
    assert old["signoff"]["receipt"] == receipt
    assert client.get(f"/api/orders/{oid}/content").text == content_before
