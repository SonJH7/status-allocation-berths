from __future__ import annotations

import numpy as np

from classical_pipeline.models import SolverConfig, SolverInput, SolverResult

try:
    import gurobipy as gp
    from gurobipy import GRB
except Exception:  # pragma: no cover - runtime dependency
    gp = None
    GRB = None


def solve_with_gurobi(data: SolverInput, config: SolverConfig) -> SolverResult:
    if gp is None or GRB is None:
        raise RuntimeError(
            "gurobipy를 찾을 수 없습니다. `pip install gurobipy` 후 Gurobi 라이선스를 확인해주세요."
        )

    vessel_ids = sorted(data.vessel_ids)
    if not vessel_ids:
        return SolverResult({}, {}, {}, "EMPTY", "최적화 대상 선박 없음")

    eta = data.eta
    ata = data.ata
    proc = data.processing_time
    s = data.vessel_length
    containers = data.containers
    berth_pos = data.berthing_position

    freezing_window: list[int] = []
    vessels_schedule: list[int] = []
    for i in vessel_ids:
        if ata.get(i) is not None:
            freezing_window.append(i)
        else:
            vessels_schedule.append(i)

    if not vessels_schedule:
        mooring = {i: int(ata[i]) for i in freezing_window if ata.get(i) is not None}
        departure = {i: int(ata[i]) + int(proc[i]) for i in mooring}
        berth = {
            i: int(berth_pos.get(i) or 0)
            for i in mooring
        }
        return SolverResult(mooring, departure, berth, "OK", "스케줄 대상 없음(모두 고정)")

    t_min_arrive = min(eta[i] for i in vessels_schedule)
    t_max_consider = max(eta[i] + proc[i] for i in vessels_schedule) + config.horizon_padding
    step = max(1, config.window_size - config.overlap)
    windows = [
        (t, min(t + config.window_size, t_max_consider))
        for t in range(t_min_arrive, t_max_consider, step)
    ]

    T = max(1, t_max_consider + config.horizon_padding + 10)
    QuayCranes = config.quay_cranes
    S = config.berth_length

    gamma_solution = np.zeros((QuayCranes, max(vessel_ids) + 1, T), dtype=int)
    zeta_solution = np.zeros((QuayCranes, max(vessel_ids) + 1, T), dtype=int)

    decision_vars: dict[int, dict[str, int]] = {}
    ETA_res: dict[int, int] = {}
    Ber_res: dict[int, int] = {}
    for i in freezing_window:
        ETA_res[i] = int(ata[i])
        Ber_res[i] = int(berth_pos.get(i) or 0)

    t_until_consider = 0
    final_status = "OK"
    message = "최적화 완료"

    for win_idx, (t_start, t_end) in enumerate(windows):
        model = gp.Model(f"Window_{win_idx + 1}")
        model.setParam("TimeLimit", config.time_limit_sec)
        model.setParam("OutputFlag", 1)
        model.setParam("LogToConsole", 1)
        print(f"[Classical] window {win_idx + 1}/{len(windows)} start: [{t_start}, {t_end}]")

        vessels_in_window = [
            i for i in vessels_schedule if eta[i] < t_end and eta[i] >= t_start
        ]
        if not vessels_in_window:
            continue

        T_max = max(eta[i] + proc[i] for i in vessels_in_window) + config.horizon_padding

        u = {}
        v = {}
        c = {}
        for i in vessels_in_window:
            u[i] = model.addVar(vtype=GRB.INTEGER, lb=eta[i], ub=T_max, name=f"x_{i}")
            v[i] = model.addVar(vtype=GRB.INTEGER, lb=1, ub=S - s[i] + 1, name=f"y_{i}")
            c[i] = model.addVar(
                vtype=GRB.INTEGER,
                lb=eta[i] + proc[i],
                ub=T_max,
                name=f"z_{i}",
            )

        sigma = {}
        delta = {}
        for i in vessels_in_window:
            for j in vessels_in_window:
                if i == j:
                    continue
                sigma[(i, j)] = model.addVar(vtype=GRB.BINARY, name=f"sigma_{i}_{j}")
                delta[(i, j)] = model.addVar(vtype=GRB.BINARY, name=f"delta_{i}_{j}")

        sigma2 = {}
        delta2 = {}
        all_vessels = freezing_window + vessels_in_window
        for i in all_vessels:
            for j in all_vessels:
                if i == j:
                    continue
                sigma2[(i, j)] = model.addVar(vtype=GRB.BINARY, name=f"sigma2_{i}_{j}")
                delta2[(i, j)] = model.addVar(vtype=GRB.BINARY, name=f"delta2_{i}_{j}")

        gamma_vars = {}
        zeta_vars = {}
        if config.use_qcap:
            for g in range(QuayCranes):
                for i in vessels_in_window:
                    for t in range(t_start, T_max):
                        gamma_vars[(g, i, t)] = model.addVar(
                            vtype=GRB.BINARY,
                            name=f"gamma_{g}_{i}_{t}",
                        )
                        zeta_vars[(g, i, t)] = model.addVar(
                            vtype=GRB.BINARY,
                            name=f"zeta_{g}_{i}_{t}",
                        )

        model.update()

        bap_objective = gp.quicksum(u[i] + proc[i] - eta[i] for i in vessels_in_window)
        model.setObjective(bap_objective, GRB.MINIMIZE)

        for i in vessels_in_window:
            model.addConstr(c[i] - u[i] == proc[i], name=f"departure_time_{i}")

        for i in vessels_in_window:
            for j in vessels_in_window:
                if i == j:
                    continue
                model.addConstr(
                    u[j] - u[i] - proc[i] - (sigma[(i, j)] - 1) * T >= 0,
                    name=f"no_overlap_time_{i}_{j}",
                )
                model.addConstr(
                    v[j] - v[i] - s[i] - (delta[(i, j)] - 1) * S >= 0,
                    name=f"no_overlap_space_{i}_{j}",
                )
                model.addConstr(
                    sigma[(i, j)] + sigma[(j, i)] + delta[(i, j)] + delta[(j, i)] >= 1,
                    name=f"no_overlap_{i}_{j}",
                )
                model.addConstr(sigma[(i, j)] + sigma[(j, i)] <= 1)
                model.addConstr(delta[(i, j)] + delta[(j, i)] <= 1)
                model.addConstr(
                    v[j]
                    - (v[i] + s[i])
                    + (sigma[(i, j)] + sigma[(j, i)]) * S
                    + (1 - delta[(i, j)]) * S
                    >= config.safety_space_gap
                )
                model.addConstr(
                    v[i]
                    - (v[j] + s[j])
                    + (sigma[(j, i)] + sigma[(i, j)]) * S
                    + (1 - delta[(j, i)]) * S
                    >= config.safety_space_gap
                )
                model.addConstr(
                    u[j]
                    - c[i]
                    + (delta[(i, j)] + delta[(j, i)]) * T
                    + (1 - sigma[(i, j)]) * T
                    >= config.safety_time_gap
                )
                model.addConstr(
                    u[i]
                    - c[j]
                    + (delta[(j, i)] + delta[(i, j)]) * T
                    + (1 - sigma[(j, i)]) * T
                    >= config.safety_time_gap
                )

        for i in vessels_in_window:
            for j in freezing_window:
                if i == j:
                    continue
                model.addConstr(ETA_res[j] - u[i] - proc[i] - (sigma2[(i, j)] - 1) * T >= 0)
                model.addConstr(Ber_res[j] - v[i] - s[i] - (delta2[(i, j)] - 1) * S >= 0)
                model.addConstr(
                    sigma2[(i, j)] + sigma2[(j, i)] + delta2[(i, j)] + delta2[(j, i)] >= 1
                )
                model.addConstr(sigma2[(i, j)] + sigma2[(j, i)] <= 1)
                model.addConstr(delta2[(i, j)] + delta2[(j, i)] <= 1)
                model.addConstr(u[i] - ETA_res[j] - proc[j] - (sigma2[(j, i)] - 1) * T >= 0)
                model.addConstr(v[i] - Ber_res[j] - s[j] - (delta2[(j, i)] - 1) * S >= 0)
                model.addConstr(
                    u[i]
                    - (ETA_res[j] + proc[j])
                    + (delta2[(j, i)] + delta2[(i, j)]) * T
                    + (1 - sigma2[(j, i)]) * T
                    >= config.safety_time_gap
                )
                model.addConstr(
                    v[i]
                    - (Ber_res[j] + s[j])
                    + (sigma2[(j, i)] + sigma2[(i, j)]) * S
                    + (1 - delta2[(j, i)]) * S
                    >= config.safety_space_gap
                )
                model.addConstr(
                    Ber_res[j]
                    - (v[i] + s[i])
                    + (sigma2[(i, j)] + sigma2[(j, i)]) * S
                    + (1 - delta2[(i, j)]) * S
                    >= config.safety_space_gap
                )

        if config.use_qcap:
            for g in range(QuayCranes):
                for t in range(t_start, T_max):
                    tempqc1 = gp.quicksum(zeta_vars[(g, i, t)] for i in vessels_in_window)
                    if t < zeta_solution.shape[2]:
                        tempqc1 += gp.quicksum(zeta_solution[(g, j, t)] for j in freezing_window)
                    model.addConstr(tempqc1 <= 1, name=f"qc1_{g}_{t}")

            for g in range(QuayCranes):
                for i in vessels_in_window:
                    for t in range(t_start, T_max):
                        model.addConstr(
                            u[i] - t * zeta_vars[(g, i, t)] - (1 - zeta_vars[(g, i, t)]) * T <= 0
                        )
                        model.addConstr(c[i] - (t + 1) * zeta_vars[(g, i, t)] >= 0)
                        model.addConstr(
                            v[i]
                            + s[i]
                            - data.rg.get(g, S) * zeta_vars[(g, i, t)]
                            - (1 - zeta_vars[(g, i, t)]) * S
                            <= 0
                        )
                        model.addConstr(v[i] - data.lg.get(g, 0) * zeta_vars[(g, i, t)] >= 0)

            for i in vessels_in_window:
                tempqc4 = gp.quicksum(
                    config.service_rate * zeta_vars[(g, i, t)]
                    for g in range(QuayCranes)
                    for t in range(t_start, T_max)
                )
                model.addConstr(tempqc4 >= containers[i], name=f"qc4_{i}")
                for t in range(t_start, T_max):
                    model.addConstr(
                        gp.quicksum(zeta_vars[(g, i, t)] for g in range(QuayCranes))
                        <= data.cmax.get(i, 4)
                    )

            for g in range(QuayCranes):
                for i in vessels_in_window:
                    for t in range(t_start + 1, T_max):
                        model.addConstr(
                            zeta_vars[(g, i, t)] - zeta_vars[(g, i, t - 1)] - gamma_vars[(g, i, t)] <= 0
                        )
                        model.addConstr(gamma_vars[(g, i, t)] + zeta_vars[(g, i, t - 1)] <= 1)
                    for t in range(t_start, T_max):
                        model.addConstr(gamma_vars[(g, i, t)] - zeta_vars[(g, i, t)] <= 0)

        model.optimize()
        t_until_consider = T_max

        if model.status not in (GRB.OPTIMAL, GRB.TIME_LIMIT):
            final_status = "INFEASIBLE"
            message = f"윈도우 {win_idx + 1}에서 해를 찾지 못했습니다."
            print(f"[Classical] window {win_idx + 1} failed: status={model.status}")
            break
        print(f"[Classical] window {win_idx + 1} done: status={model.status}")

        freezing_window_qc: list[int] = []
        for i in vessels_in_window:
            decision_vars[i] = {
                "Mooring Time": int(round(u[i].X)),
                "Berth Section": int(round(v[i].X)),
                "Departure Time": int(round(c[i].X)),
            }
            if eta[i] <= t_end - config.overlap:
                freezing_window.append(i)
                freezing_window_qc.append(i)

        if config.use_qcap:
            if win_idx < len(windows) - 1:
                if freezing_window_qc:
                    t_window_end = max(
                        int(decision_vars[i]["Mooring Time"]) + proc[i] for i in freezing_window_qc
                    )
                else:
                    t_window_end = t_until_consider
                for g in range(QuayCranes):
                    for i in freezing_window_qc:
                        for t in range(t_start, min(t_window_end, zeta_solution.shape[2])):
                            gamma_solution[g][i][t] = int(round(gamma_vars[(g, i, t)].X))
                            zeta_solution[g][i][t] = int(round(zeta_vars[(g, i, t)].X))
            else:
                for g in range(QuayCranes):
                    for i in vessels_in_window:
                        for t in range(t_start, min(T_max, zeta_solution.shape[2])):
                            gamma_solution[g][i][t] = int(round(gamma_vars[(g, i, t)].X))
                            zeta_solution[g][i][t] = int(round(zeta_vars[(g, i, t)].X))

        for i in vessels_in_window:
            ETA_res[i] = int(decision_vars[i]["Mooring Time"])
            Ber_res[i] = int(decision_vars[i]["Berth Section"])

    mooring_slot = {}
    departure_slot = {}
    berth_section = {}
    for i in vessel_ids:
        if i in decision_vars:
            mooring_slot[i] = int(decision_vars[i]["Mooring Time"])
            departure_slot[i] = int(decision_vars[i]["Departure Time"])
            berth_section[i] = int(decision_vars[i]["Berth Section"])
        elif i in ETA_res:
            mooring_slot[i] = int(ETA_res[i])
            departure_slot[i] = int(ETA_res[i] + proc[i])
            berth_section[i] = int(Ber_res.get(i, berth_pos.get(i) or 0))

    return SolverResult(
        mooring_slot=mooring_slot,
        departure_slot=departure_slot,
        berth_section=berth_section,
        status=final_status,
        message=message,
    )
