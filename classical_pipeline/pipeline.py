from __future__ import annotations

import pandas as pd

from classical_pipeline.adapter_in import build_solver_input
from classical_pipeline.adapter_out import build_visualization_df
from classical_pipeline.models import SolverConfig
from classical_pipeline.solver_gurobi import solve_with_gurobi


def run_classical_pipeline(df: pd.DataFrame, config: SolverConfig) -> tuple[pd.DataFrame, dict]:
    solver_input = build_solver_input(df, config)
    solver_result = solve_with_gurobi(solver_input, config)
    out_df = build_visualization_df(solver_input, solver_result)
    meta = {
        "status": solver_result.status,
        "message": solver_result.message,
        "n_in": len(solver_input.vessel_ids),
        "n_out": len(solver_result.mooring_slot),
    }
    return out_df, meta
