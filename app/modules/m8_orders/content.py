"""冻结内容：签发时把迁移单正文定稿，更正只在前头加更正说明。

内容一旦写进单据就不再变；原分析结果（批次载荷）也不被触碰。
"""

from __future__ import annotations

from datetime import datetime

from app import config
from app.manifest import sha256_bytes
from app.modules.m7_migration import service as migration


def freeze(run: dict, *, revision: int, supersedes: str | None, reason: str | None,
           amendments: list[dict], issued_at: str) -> tuple[str, str]:
    """渲染冻结正文并给出内容摘要。首版即原迁移单，更正版前置更正说明。"""
    base = migration.spec_markdown(run)
    if revision <= 1:
        text = base
    else:
        head = [
            f"# 更正说明（第 {revision - 1} 次更正）",
            "",
            f"- 原单号：{supersedes}（已作废，旧内容与签收凭据保留在原单）",
            f"- 更正原因：{reason}",
            f"- 更正时间：{_local(issued_at)}",
            "",
            "## 更正条款",
            "",
        ]
        if amendments:
            for index, item in enumerate(amendments, start=1):
                clause = f"[{item['clause']}] " if item.get("clause") else ""
                head.append(f"{index}. {clause}{item['text']}")
        else:
            head.append("（未逐条列更正项，条款以正文为准，更正原因见上）")
        head.extend(["", "---", ""])
        text = "\n".join(head) + base
    return text, sha256_bytes(text.encode("utf-8"))


def _local(iso_text: str) -> str:
    try:
        moment = datetime.fromisoformat(iso_text)
    except ValueError:
        return iso_text
    return config.to_local_text(moment)
