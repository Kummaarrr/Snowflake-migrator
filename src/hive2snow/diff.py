"""Compare a source result with a target result: the proof that a conversion is right.

Rules:
- same column count (names may differ: engines label expressions differently), same row count;
- cells compared by position after normalisation: numbers within a tolerance (1e-6 relative or
  0.005 absolute, whichever is larger: DECIMAL vs DOUBLE rounding), strings stripped, dates and
  timestamps as ISO strings, NULLs equal to NULLs, booleans as 0/1;
- if the query orders its output, rows are compared in order; otherwise both sides are sorted
  canonically first (order-insensitive);
- the verdict carries a short, specific explanation the agent can act on (which column, which
  row, which values).
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pandas as pd

MAX_DIFF_ROWS = 5


@dataclass
class Diff:
    match: bool
    summary: str  # one line
    details: list[str] = field(default_factory=list)  # first mismatches, for the agent
    source_rows: int = 0
    target_rows: int = 0

    def feedback(self) -> str:
        return self.summary + ("\n" + "\n".join(self.details) if self.details else "")


def _norm(v: Any) -> Any:
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int | float | Decimal):
        f = float(v)
        return None if math.isnan(f) else f
    if isinstance(v, pd.Timestamp | dt.datetime):
        return (
            v.strftime("%Y-%m-%d %H:%M:%S")
            if (v.hour or v.minute or v.second)
            else v.strftime("%Y-%m-%d")
        )
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, list | tuple | dict):
        return json.dumps(v, default=str, separators=(",", ":"), sort_keys=True)
    s = str(v).strip()
    if re.fullmatch(r"-?\d+\.\d+", s):  # numeric text from a VARIANT or a string cast
        return float(s)
    if s[:1] in "[{" and s[-1:] in "]}":  # arrays / objects: engines print JSON differently
        try:
            return json.dumps(json.loads(s), separators=(",", ":"), sort_keys=True)
        except ValueError:
            return s
    return s


def _cell_equal(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is b
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) <= max(0.005, 1e-6 * max(abs(a), abs(b)))
    if isinstance(a, float) != isinstance(b, float):
        try:
            return _cell_equal(float(a), float(b))
        except (TypeError, ValueError):
            return False
    return a == b


def _sort_key(row: tuple) -> tuple:
    return tuple(
        (v is None, "" if v is None else (0, v) if isinstance(v, float) else (1, str(v)))
        for v in row
    )


def compare(
    source: pd.DataFrame,
    target: pd.DataFrame,
    ordered: bool,
    order_positions: list[int] | None = None,
) -> Diff:
    """`order_positions`: output positions of the ORDER BY keys when they are known. Then an
    ordered compare checks the key sequence row by row and the full rows as a multiset, so rows
    tied on the keys may come in either order (both engines are right); without positions an
    ordered compare is strict, row by row."""
    s_rows = [tuple(_norm(v) for v in row) for row in source.itertuples(index=False, name=None)]
    t_rows = [tuple(_norm(v) for v in row) for row in target.itertuples(index=False, name=None)]
    if ordered and order_positions and source.shape[1] == target.shape[1]:
        if len(s_rows) == len(t_rows):
            for i, (a, b) in enumerate(zip(s_rows, t_rows, strict=True)):
                for j in order_positions:
                    if j < len(a) and not _cell_equal(a[j], b[j]):
                        col = source.columns[j]
                        return Diff(
                            False,
                            f"order differs at row {i + 1}: sort key {col} is {a[j]!r} in the "
                            f"source, {b[j]!r} in the target",
                            [],
                            len(s_rows),
                            len(t_rows),
                        )
        ordered = False  # keys agree in sequence: compare the rows as a multiset
    if source.shape[1] != target.shape[1]:
        return Diff(
            False,
            f"column count differs: source {source.shape[1]} "
            f"({', '.join(map(str, source.columns))}) vs target {target.shape[1]} "
            f"({', '.join(map(str, target.columns))})",
            [],
            len(s_rows),
            len(t_rows),
        )
    if len(s_rows) != len(t_rows):
        return Diff(
            False,
            f"row count differs: source {len(s_rows)} vs target {len(t_rows)}",
            _examples(s_rows, t_rows, source.columns),
            len(s_rows),
            len(t_rows),
        )
    if not ordered:
        s_rows, t_rows = sorted(s_rows, key=_sort_key), sorted(t_rows, key=_sort_key)
    bad: list[str] = []
    for i, (a, b) in enumerate(zip(s_rows, t_rows, strict=True)):
        for j, (x, y) in enumerate(zip(a, b, strict=True)):
            if not _cell_equal(x, y):
                col = source.columns[j]
                bad.append(f"row {i + 1}, column {j + 1} ({col}): source {x!r} vs target {y!r}")
                break
        if len(bad) >= MAX_DIFF_ROWS:
            break
    if bad:
        total = sum(
            1
            for a, b in zip(s_rows, t_rows, strict=True)
            if any(not _cell_equal(x, y) for x, y in zip(a, b, strict=True))
        )
        return Diff(False, f"{total} of {len(s_rows)} rows differ", bad, len(s_rows), len(t_rows))
    return Diff(
        True, f"{len(s_rows)} rows, {source.shape[1]} columns match", [], len(s_rows), len(t_rows)
    )


def _examples(s_rows: list[tuple], t_rows: list[tuple], columns: Any) -> list[str]:
    s_set, t_set = set(map(_hashable, s_rows)), set(map(_hashable, t_rows))
    only_s = [r for r in s_rows if _hashable(r) not in t_set][:3]
    only_t = [r for r in t_rows if _hashable(r) not in s_set][:3]
    out = [f"columns: {', '.join(map(str, columns))}"]
    out += [f"only in source: {r}" for r in only_s]
    out += [f"only in target: {r}" for r in only_t]
    return out


def _hashable(row: tuple) -> tuple:
    return tuple(round(v, 2) if isinstance(v, float) else v for v in row)


def has_order_by(sql: str) -> bool:
    """Does the outermost statement order its result? (ORDER BY inside subqueries / CTEs or
    window functions doesn't count.)"""
    depth, i, text = 0, 0, sql
    last = None
    while i < len(text):
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and text[i : i + 8].upper() == "ORDER BY":
            last = i
        i += 1
    if last is None:
        return False
    tail = text[last:].upper()
    return (
        not tail.lstrip().startswith("ORDER BY")
        or "OVER" not in text[max(0, last - 40) : last].upper()
    )
