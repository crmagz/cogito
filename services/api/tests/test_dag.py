from __future__ import annotations

from cogito_api.dag import validate_constraints, validate_phase_dag
from cogito_api.models import PlanConstraints, PlanPhase

from .conftest import make_settings


def _phase(id: str, depends_on: list[str] | None = None) -> PlanPhase:
    return PlanPhase(
        id=id,
        name=id,
        description="d",
        tasks=["t"],
        acceptance_criteria=["a"],
        verification=["v"],
        depends_on=depends_on or [],
    )


def test_valid_linear_dag_has_no_violations():
    phases = [_phase("phase-1"), _phase("phase-2", ["phase-1"])]
    assert validate_phase_dag(phases) == []


def test_unknown_dependency_is_a_violation():
    phases = [_phase("phase-1", ["phase-3"])]
    violations = validate_phase_dag(phases)
    assert len(violations) == 1
    assert "non-existent phase 'phase-3'" in violations[0].message


def test_self_loop_is_detected_as_cycle():
    phases = [_phase("phase-1", ["phase-1"])]
    violations = validate_phase_dag(phases)
    assert len(violations) == 1
    assert "cycle" in violations[0].message


def test_two_node_cycle_is_detected():
    phases = [_phase("phase-1", ["phase-2"]), _phase("phase-2", ["phase-1"])]
    violations = validate_phase_dag(phases)
    assert len(violations) == 1
    assert "cycle" in violations[0].message


def test_constraints_within_bounds_pass():
    settings = make_settings()
    constraints = PlanConstraints(max_wall_clock_minutes=60, max_cost_usd=5.0)
    assert validate_constraints(constraints, settings) == []


def test_constraints_exceeding_bounds_are_reported():
    settings = make_settings(max_cost_usd=5.0)
    constraints = PlanConstraints(max_cost_usd=100.0)
    violations = validate_constraints(constraints, settings)
    assert len(violations) == 1
    assert violations[0].field == "constraints.max_cost_usd"
