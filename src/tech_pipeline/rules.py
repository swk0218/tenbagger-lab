from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class GateRule:
    field: str
    op: str
    threshold: float


@dataclass
class RuleSpec:
    step_a: list[GateRule]
    step_b: dict[str, float]


def load_rules(path: str) -> RuleSpec:
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    step_a: list[GateRule] = []
    step_b: dict[str, float] = {}

    in_a = False
    in_b = False
    for raw in text.splitlines():
        line = raw.strip()
        if line.lower().startswith("## step a"):
            in_a, in_b = True, False
            continue
        if line.lower().startswith("## step b"):
            in_a, in_b = False, True
            continue

        if in_a:
            m = re.match(r"^-\s*([a-zA-Z0-9_]+)\s*(>=|<=|>|<|==)\s*([0-9.]+)", line)
            if m:
                step_a.append(GateRule(m.group(1), m.group(2), float(m.group(3))))
        elif in_b:
            m = re.match(r"^-\s*([a-zA-Z0-9_]+):\s*([0-9.]+)", line)
            if m:
                step_b[m.group(1)] = float(m.group(2))

    if not step_a:
        step_a = [GateRule("adtv_3m_usd", ">=", 1.0)]
    if not step_b:
        step_b = {"rs126": 1.0}

    total = sum(step_b.values())
    if total > 0:
        step_b = {k: v / total for k, v in step_b.items()}

    return RuleSpec(step_a=step_a, step_b=step_b)


def evaluate_gate(value: float | None, op: str, threshold: float) -> bool:
    if value is None:
        return False
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    if op == ">":
        return value > threshold
    if op == "<":
        return value < threshold
    if op == "==":
        return value == threshold
    return False
