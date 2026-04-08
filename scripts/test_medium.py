"""
Test script for MEDIUM task: aggregation_analysis
Runs a full episode against the live HF Space or local server.

Usage:
    uv run python scripts/test_medium.py
    uv run python scripts/test_medium.py --local   # test against localhost:8000
"""

import asyncio
import json
import sys
import time

import websockets

SPACE_WS = "wss://amritesh29-sql-query-env.hf.space/ws"
LOCAL_WS = "ws://localhost:8000/ws"

TASK_ID = "aggregation_analysis"
DIFFICULTY = "MEDIUM"

# Scripted steps simulating an agent's iterative approach
STEPS = [
    (
        "execute",
        "SELECT d.name, COUNT(e.id), AVG(e.salary) "
        "FROM departments d "
        "JOIN employees e ON e.department_id = d.id "
        "GROUP BY d.id",
        "ATTEMPT 1: Basic aggregation — missing project join, HAVING, aliases",
    ),
    (
        "execute",
        "SELECT d.name AS department_name, COUNT(e.id) AS employee_count, "
        "ROUND(AVG(e.salary), 2) AS avg_salary, COUNT(p.id) AS active_project_count "
        "FROM departments d "
        "JOIN employees e ON e.department_id = d.id "
        "JOIN projects p ON p.department_id = d.id AND p.status = 'active' "
        "GROUP BY d.id HAVING AVG(e.salary) > 80000 "
        "ORDER BY avg_salary DESC",
        "ATTEMPT 2: Added project JOIN but missing DISTINCT — row duplication bug",
    ),
    (
        "execute",
        "SELECT d.name AS department_name, COUNT(DISTINCT e.id) AS employee_count, "
        "ROUND(AVG(e.salary), 2) AS avg_salary, COUNT(DISTINCT p.id) AS active_project_count "
        "FROM departments d "
        "JOIN employees e ON e.department_id = d.id "
        "JOIN projects p ON p.department_id = d.id AND p.status = 'active' "
        "GROUP BY d.id, d.name "
        "HAVING AVG(e.salary) > 80000 "
        "ORDER BY avg_salary DESC",
        "ATTEMPT 3: Fixed with DISTINCT — should match expected result",
    ),
    (
        "submit",
        "SELECT d.name AS department_name, COUNT(DISTINCT e.id) AS employee_count, "
        "ROUND(AVG(e.salary), 2) AS avg_salary, COUNT(DISTINCT p.id) AS active_project_count "
        "FROM departments d "
        "JOIN employees e ON e.department_id = d.id "
        "JOIN projects p ON p.department_id = d.id AND p.status = 'active' "
        "GROUP BY d.id, d.name "
        "HAVING AVG(e.salary) > 80000 "
        "ORDER BY avg_salary DESC",
        "SUBMIT — final answer",
    ),
]


def log(level, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


async def run():
    use_local = "--local" in sys.argv
    ws_url = LOCAL_WS if use_local else SPACE_WS
    target = "localhost:8000" if use_local else "HF Space"

    print("=" * 70)
    print(f"  MEDIUM TEST: {TASK_ID}")
    print(f"  Target: {target}")
    print("=" * 70)

    async with websockets.connect(ws_url) as ws:
        # ── RESET ──
        log("RESET", f"Sending reset(task_id={TASK_ID})")
        await ws.send(json.dumps({"type": "reset", "data": {"task_id": TASK_ID}}))
        resp = json.loads(await ws.recv())
        obs = resp["data"]

        log("RESET", f"done={obs.get('done')}, reward={obs.get('reward')}")
        log("RESET", f"Schema length: {len(obs.get('schema_description', '') or '')} chars")
        log("RESET", f"Question: {obs.get('task_description', 'N/A')}")
        print()

        # ── STEPS ──
        rewards = []
        for i, (action_type, query, description) in enumerate(STEPS, 1):
            log(f"STEP {i}", f">>> {description}")
            log(f"STEP {i}", f"action_type={action_type}")
            log(f"STEP {i}", f"query={query[:120]}{'...' if len(query) > 120 else ''}")

            await ws.send(json.dumps({
                "type": "step",
                "data": {"action_type": action_type, "query": query},
            }))
            resp = json.loads(await ws.recv())
            obs = resp["data"]

            reward = obs.get("reward", 0)
            done = obs.get("done", False)
            error = obs.get("error_message")
            result = obs.get("query_result", "") or ""
            row_count = obs.get("row_count", 0)
            columns = obs.get("column_names", [])
            rewards.append(reward)

            log(f"STEP {i}", f"reward={reward:.2f}, done={done}")

            if error:
                log(f"STEP {i}", f"ERROR: {error}")

            if result:
                lines = result.strip().split("\n")
                log(f"STEP {i}", f"Result: {row_count} rows, columns={columns}")
                for line in lines[:8]:
                    print(f"           {line}")
                if len(lines) > 8:
                    print(f"           ... ({len(lines)} lines total)")

            print()

            if done:
                break

        # ── STATE ──
        await ws.send(json.dumps({"type": "state"}))
        resp = json.loads(await ws.recv())
        state = resp["data"]

        # ── SUMMARY ──
        final_score = rewards[-1] if rewards else 0.0
        print("=" * 70)
        print(f"  RESULT: {TASK_ID} ({DIFFICULTY})")
        print(f"  Steps taken:       {len(rewards)}")
        print(f"  Rewards per step:  {', '.join(f'{r:.2f}' for r in rewards)}")
        print(f"  Final score:       {final_score:.2f}")
        print(f"  Submitted:         {state.get('submitted', False)}")
        print(f"  Cumulative reward: {state.get('cumulative_reward', 0)}")
        print(f"  Status:            {'PASS' if final_score >= 0.5 else 'FAIL'}")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run())
