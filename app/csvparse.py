"""Strict CSV parsing, canonicalisation, and UTC-timestamp handling.

The parser is deliberately unforgiving: a submitted batch is accepted in full
or rejected in full. Every problem is collected so the operator sees the
complete diagnosis in one pass.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

UTC = timezone.utc
MIN_EPOCH = -62_135_596_800   # 0001-01-01T00:00:00Z
MAX_EPOCH = 253_402_300_799   # 9999-12-31T23:59:59Z

TRANSACTION_HEADERS = [
    "event_id", "account_id", "occurred_at", "ingested_at",
    "amount", "action",
]
SAMPLE_HEADERS = ["sample_id", "account_id", "audit_at", "label"]

TRANSACTION_ALIASES = {
    "event_id": {"event_id", "event", "事件号", "事件id", "事件编号"},
    "account_id": {"account_id", "account", "账户", "账户号", "账号"},
    "occurred_at": {"occurred_at", "occurred", "发生时刻", "发生时间", "交易时刻"},
    "ingested_at": {"ingested_at", "ingested", "源系统入库时刻", "入库时刻",
                    "入库时间", "入湖时刻"},
    "amount": {"amount", "金额", "交易金额"},
    "action": {"action", "更新/撤销", "操作", "动作", "更新撤销"},
}
SAMPLE_ALIASES = {
    "sample_id": {"sample_id", "sample", "样本号", "样本id", "样本编号"},
    "account_id": {"account_id", "account", "账户", "账户号", "账号"},
    "audit_at": {"audit_at", "audit", "审核时刻", "审核时间", "审核点"},
    "label": {"label", "标签", "目标", "y"},
}
ACTION_REVISE = "revise"
ACTION_CANCEL = "cancel"

_ASCII_WS = re.compile(r"[ \t\r\n\f\v]+")


class ParseError(Exception):
    """Base class for upload failures that map to HTTP 422."""

    code = "invalid_csv"

    def __init__(self, message: str, details: list[dict] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or []

    def payload(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


class BatchRejected(ParseError):
    """The whole batch is refused because of one or more row-level problems."""

    code = "batch_rejected"

    def __init__(self, message: str, details: list[dict]):
        super().__init__(message, details)


@dataclass(frozen=True)
class TransactionRevision:
    row_number: int
    event_id: str
    account_id: str
    occurred_at: int
    ingested_at: int
    amount: Decimal
    amount_minor: int
    amount_text: str
    action: str
    raw_hash: str


@dataclass(frozen=True)
class SampleRow:
    row_number: int
    sample_id: str
    account_id: str
    audit_at: int
    label: int


@dataclass
class ParsedBatch:
    transactions: list[TransactionRevision] = field(default_factory=list)


@dataclass
class ParsedSamples:
    samples: list[SampleRow] = field(default_factory=list)


def _row_error(row: int, field_name: str, code: str, message: str, value: str = "") -> dict:
    return {"row": row, "field": field_name, "code": code,
            "message": message, "value": value}


def _read_table(data: bytes, expected: list[str], kind: str,
                aliases: dict[str, set[str]] | None = None) -> list[dict]:
    """Decode strictly as UTF-8 (with optional BOM) and validate the header."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise BatchRejected(
            f"{kind} CSV 必须是 UTF-8 编码（可带 BOM）。",
            [_row_error(1, "", "encoding", "无法按 UTF-8 解码，可能为 GBK/Excel 导出。")],
        )
    try:
        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        rows = list(reader)
    except csv.Error as exc:
        raise BatchRejected(
            f"{kind} CSV 格式损坏，无法解析：{exc}",
            [_row_error(0, "", "malformed_csv", str(exc))],
        )
    if not rows:
        raise BatchRejected(f"{kind} CSV 为空。",
                            [_row_error(1, "", "empty", "未找到表头。")])
    header = [cell.lstrip("﻿").strip() for cell in rows[0]]
    if aliases is None:
        if header != expected:
            raise BatchRejected(
                f"{kind} CSV 表头必须严格为：{', '.join(expected)}。",
                [_row_error(1, "header", "header", "表头不符合约定",
                            ", ".join(header))],
            )
        column_map = {name: i for i, name in enumerate(expected)}
    else:
        normalized = {}
        for i, cell in enumerate(header):
            key = cell.strip().lower().replace(" ", "").replace("-", "_")
            canonical = None
            for name, names in aliases.items():
                if key in {n.lower().replace(" ", "").replace("-", "_") for n in names}:
                    canonical = name
                    break
            if canonical is None:
                continue  # extra columns are tolerated and ignored
            if canonical in normalized:
                raise BatchRejected(
                    f"{kind} CSV 表头含重复列 {canonical}。",
                    [_row_error(1, canonical, "duplicate_header",
                                f"第 {normalized[canonical] + 1} 列与第 {i + 1} 列重复。")],
                )
            normalized[canonical] = i
        missing = [name for name in expected if name not in normalized]
        if missing:
            raise BatchRejected(
                f"{kind} CSV 缺少必需列：{', '.join(missing)}。",
                [_row_error(1, "header", "missing_header",
                            f"缺少列：{', '.join(missing)}；实际表头：{', '.join(header)}")],
            )
        column_map = normalized
    width = len(header)
    out = []
    for row_number, cells in enumerate(rows[1:], start=2):
        if not cells or all(not c.strip() for c in cells):
            continue
        if len(cells) != width:
            out.append({"__row__": row_number,
                        "__error__": _row_error(row_number, "", "column_count",
                                                f"应有 {width} 列，实际 {len(cells)} 列。")})
            continue
        out.append({"__row__": row_number,
                    **{name: cells[idx] for name, idx in column_map.items()}})
    # CSV uses LF/CRLF internally; reject physical NULs outright.
    if "\x00" in text:
        raise BatchRejected(f"{kind} CSV 含 NUL 字符。",
                            [_row_error(0, "", "nul_byte", "文件包含 NUL 字符。")])
    return out


def _parse_text(value: str, row: int, name: str, issues: list[dict],
                label: str, max_len: int = 128) -> str:
    v = value.strip()
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return ""
    if len(v) > max_len:
        issues.append(_row_error(row, name, "too_long",
                                 f"{label}超过 {max_len} 字符。", v[:40]))
        return ""
    return v


def _parse_epoch(value: str, row: int, name: str, issues: list[dict],
                 label: str) -> int | None:
    v = value.strip()
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return None
    if not re.fullmatch(r"-?\d{1,11}", v):
        issues.append(_row_error(
            row, name, "not_utc_seconds",
            f"{label}必须是 UTC 秒整数（如 1736071200），不接受日期字符串或小数。", v))
        return None
    n = int(v)
    if not (MIN_EPOCH <= n <= MAX_EPOCH):
        issues.append(_row_error(row, name, "out_of_range",
                                 f"{label}超出可表示范围。", v))
        return None
    return n


def _parse_decimal(value: str, row: int, name: str, issues: list[dict],
                   label: str) -> Decimal | None:
    v = _ASCII_WS.sub("", value)
    if not v:
        issues.append(_row_error(row, name, "required", f"{label}不能为空。"))
        return None
    # Plain decimal only: no scientific notation, no thousands separators,
    # no currency symbols. At most two fractional digits keeps the stored
    # integer minor-unit value exact.
    if not re.fullmatch(r"-?\d{1,15}(?:\.\d{1,2})?", v):
        issues.append(_row_error(
            row, name, "not_decimal",
            f"{label}必须是十进制金额（最多两位小数，如 125.00），且不得为科学计数法。", v))
        return None
    try:
        d = Decimal(v)
    except InvalidOperation:  # pragma: no cover - regex already guards this
        issues.append(_row_error(row, name, "not_decimal", f"{label}无法解析。", v))
        return None
    if d < 0:
        issues.append(_row_error(row, name, "amount_negative",
                                 f"{label}不能为负。", v))
        return None
    return d


def _parse_action(value: str, row: int, issues: list[dict]) -> str | None:
    v = value.strip().lower()
    if v in (ACTION_REVISE, "update", "correct", "修订", "更新"):
        return ACTION_REVISE
    if v in (ACTION_CANCEL, "void", "reverse", "撤销", "作废"):
        return ACTION_CANCEL
    if not v:
        issues.append(_row_error(row, "action", "required", "action 不能为空。"))
    else:
        issues.append(_row_error(row, "action", "invalid_action",
                                 "action 只能是 revise/update/correct 或 cancel/void/撤销。", value))
    return None


def canonical_raw_hash(fields: list[str]) -> str:
    """Hash the *semantic* revision identity.

    Same event + same ingested_at must carry identical content. We hash the
    parsed semantic values rather than raw bytes so insignificant whitespace,
    quote style, or CRLF differences do not create phantom conflicts.
    """
    h = hashlib.sha256()
    for part in fields:
        h.update(part.encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def parse_transactions(data: bytes) -> ParsedBatch:
    rows = _read_table(data, TRANSACTION_HEADERS, "交易", TRANSACTION_ALIASES)
    issues: list[dict] = []
    txns: list[TransactionRevision] = []
    for row in rows:
        if "__error__" in row:
            issues.append(row["__error__"])
            continue
        n = row["__row__"]
        event_id = _parse_text(row["event_id"], n, "event_id", issues, "事件号")
        account_id = _parse_text(row["account_id"], n, "account_id", issues, "账户")
        occurred = _parse_epoch(row["occurred_at"], n, "occurred_at", issues, "发生时刻")
        ingested = _parse_epoch(row["ingested_at"], n, "ingested_at", issues, "入库时刻")
        amount = _parse_decimal(row["amount"], n, "amount", issues, "金额")
        action = _parse_action(row["action"], n, issues)
        if action == ACTION_REVISE and amount is not None and amount == 0:
            issues.append(_row_error(n, "amount", "amount_positive",
                                     "更新交易的金额必须大于 0。", row["amount"]))
            continue
        if not (event_id and account_id and occurred is not None
                and ingested is not None and amount is not None and action):
            continue
        txn = TransactionRevision(
            row_number=n, event_id=event_id, account_id=account_id,
            occurred_at=occurred, ingested_at=ingested, amount=amount,
            amount_minor=int(amount * 100),
            amount_text=format(amount, ".2f"),
            action=action,
            raw_hash=canonical_raw_hash([
                event_id, account_id, str(occurred), str(ingested),
                format(amount, ".2f"), action,
            ]),
        )
        txns.append(txn)
    if issues:
        raise BatchRejected(
            f"交易批次整批拒收：共 {len(rows)} 行，发现 {len(issues)} 处问题，未写入任何数据。",
            issues,
        )
    if not txns:
        raise BatchRejected("交易 CSV 没有有效数据行。",
                            [_row_error(2, "", "no_rows", "至少需要一行交易。")])
    return ParsedBatch(transactions=txns)


def parse_samples(data: bytes) -> ParsedSamples:
    rows = _read_table(data, SAMPLE_HEADERS, "样本", SAMPLE_ALIASES)
    issues: list[dict] = []
    samples: list[SampleRow] = []
    for row in rows:
        if "__error__" in row:
            issues.append(row["__error__"])
            continue
        n = row["__row__"]
        sample_id = _parse_text(row["sample_id"], n, "sample_id", issues, "样本号")
        account_id = _parse_text(row["account_id"], n, "account_id", issues, "账户")
        audit = _parse_epoch(row["audit_at"], n, "audit_at", issues, "审核时刻")
        label_raw = row["label"].strip()
        label = None
        if not label_raw:
            issues.append(_row_error(n, "label", "required", "标签不能为空。"))
        elif not re.fullmatch(r"0|1", label_raw):
            issues.append(_row_error(n, "label", "invalid_label",
                                     "标签只能是 0 或 1。", row["label"]))
        else:
            label = int(label_raw)
        if not (sample_id and account_id and audit is not None and label is not None):
            continue
        samples.append(SampleRow(n, sample_id, account_id, audit, label))
    if issues:
        raise BatchRejected(
            f"样本批次整批拒收：共 {len(rows)} 行，发现 {len(issues)} 处问题，未写入任何数据。",
            issues,
        )
    if not samples:
        raise BatchRejected("样本 CSV 没有有效数据行。",
                            [_row_error(2, "", "no_rows", "至少需要一行样本。")])
    seen: dict[str, int] = {}
    dupes = []
    for s in samples:
        if s.sample_id in seen:
            dupes.append(_row_error(s.row_number, "sample_id", "duplicate_sample_id",
                                    f"样本号与第 {seen[s.sample_id]} 行重复。"))
        else:
            seen[s.sample_id] = s.row_number
    if dupes:
        raise BatchRejected("样本批次整批拒收：样本号在同一上传中必须唯一。", dupes)
    return ParsedSamples(samples=samples)


def fmt_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


WINDOW_SECONDS = 7 * 24 * 3600
