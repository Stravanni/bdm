from blend.Operators.Seekers import Correlation
from blend.Plan import Plan

# typing imports
from typing import List
from numbers import Number


def CorrelationSearch(source_column: List[str], target_column: List[Number], k: int = 10) -> Plan:
    plan = Plan()
    plan.add("correlation", Correlation(source_column, target_column, k))
    return plan




