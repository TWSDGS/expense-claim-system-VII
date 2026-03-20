from __future__ import annotations

from typing import Iterable, List

SHARED_PLAN_CODE_OPTIONS: list[str] = [
    "115CA006",
    "115CA008",
    "115CA011",
    "115CA012",
    "115GO003",
    "115GO004",
    "115GO005",
    "115ID003",
    "115IS002",
    "115GA002",
    "115GA003",
]


def get_shared_plan_code_options(extra_values: Iterable[str] | None = None, include_other: bool = True) -> List[str]:
    values: List[str] = []
    for v in SHARED_PLAN_CODE_OPTIONS:
        if v not in values:
            values.append(v)
    for v in extra_values or []:
        s = str(v).strip()
        if s and s not in values:
            values.append(s)
    if include_other and "其他" not in values:
        values.append("其他")
    return values
