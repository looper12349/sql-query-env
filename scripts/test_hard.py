"""
Test script for HARD task: complex_analytics
Runs a full episode against the live HF Space or local server.

Usage:
    uv run python scripts/test_hard.py
    uv run python scripts/test_hard.py --local   # test against localhost:8000
"""

import asyncio
import json
import sys
import time

import websockets

SPACE_WS = "wss://amritesh29-sql-query-env.hf.space/ws"
LOCAL_WS = "ws://localhost:8000/ws"

TASK_ID = "complex_analytics"
DIFFICULTY = "HARD"

# Scripted steps simulating a struggling agent on the hard task
STEPS = [
    (
        "execute",
        "SELECT e.name, SUM(a.hours_per_week) as total_hours "
        "FROM employees e "
        "JOIN assignments a ON a.employee_id = e.id "
        "GROUP BY e.id "
        "ORDER BY total_hours DESC",
        "ATTEMPT 1: Basic hours aggregation — no dept filter, no rank, no project count",
    ),
    (
        "execute",
        "SELECT d.name, e.name, SUM(a.hours_per_week) as total_hours, "
        "COUNT(DISTINCT a.project_id) as num_projects "
        "FROM employees e "
        "JOIN assignments a ON a.employee_id = e.id "
        "JOIN departments d ON d.id = e.department_id "
        "GROUP BY e.id "
        "ORDER BY total_hours DESC",
        "ATTEMPT 2: Added dept + project count — still missing salary_rank and dept filter",
    ),
    (
        "execute",
        "SELECT d.name, e.name, SUM(a.hours_per_week) as total_hours, "
        "RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) as salary_rank "
        "FROM employees e "
        "JOIN assignments a ON a.employee_id = e.id "
        "JOIN departments d ON d.id = e.department_id "
        "GROUP BY e.id "
        "ORDER BY total_hours DESC",
        "ATTEMPT 3: Added window function — but mixing aggregate + window is wrong in SQLite",
    ),
    (
        "execute",
        "WITH employee_hours AS ("
        "  SELECT e.id AS employee_id, e.name AS employee_name, e.department_id, e.salary, "
        "    SUM(a.hours_per_week) AS total_hours, "
        "    COUNT(DISTINCT a.project_id) AS num_projects "
        "  FROM employees e "
        "  JOIN assignments a ON a.employee_id = e.id "
        "  GROUP BY e.id, e.name, e.department_id, e.salary"
        "), "
        "dept_employee_count AS ("
        "  SELECT department_id, COUNT(DISTINCT employee_id) AS emp_count "
        "  FROM assignments a "
        "  JOIN employees e ON e.id = a.employee_id "
        "  GROUP BY department_id "
        "  HAVING COUNT(DISTINCT employee_id) >= 2"
        "), "
        "salary_ranks AS ("
        "  SELECT e.id AS employee_id, e.department_id, "
        "    DENSE_RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank "
        "  FROM employees e"
        "), "
        "ranked AS ("
        "  SELECT eh.*, sr.salary_rank, "
        "    ROW_NUMBER() OVER ("
        "      PARTITION BY eh.department_id "
        "      ORDER BY eh.total_hours DESC, eh.salary DESC"
        "    ) AS rn "
        "  FROM employee_hours eh "
        "  JOIN dept_employee_count dc ON dc.department_id = eh.department_id "
        "  JOIN salary_ranks sr ON sr.employee_id = eh.employee_id"
        ") "
        "SELECT d.name AS department_name, r.employee_name, r.total_hours, "
        "  r.num_projects, r.salary_rank "
        "FROM ranked r "
        "JOIN departments d ON d.id = r.department_id "
        "WHERE r.rn = 1 "
        "ORDER BY r.total_hours DESC",
        "ATTEMPT 4: Full CTE with DENSE_RANK + ROW_NUMBER + dept filter — CORRECT",
    ),
    (
        "submit",
        "WITH employee_hours AS ("
        "  SELECT e.id AS employee_id, e.name AS employee_name, e.department_id, e.salary, "
        "    SUM(a.hours_per_week) AS total_hours, "
        "    COUNT(DISTINCT a.project_id) AS num_projects "
        "  FROM employees e "
        "  JOIN assignments a ON a.employee_id = e.id "
        "  GROUP BY e.id, e.name, e.department_id, e.salary"
        "), "
        "dept_employee_count AS ("
        "  SELECT department_id, COUNT(DISTINCT employee_id) AS emp_count "
        "  FROM assignments a "
        "  JOIN employees e ON e.id = a.employee_id "
        "  GROUP BY department_id "
        "  HAVING COUNT(DISTINCT employee_id) >= 2"
        "), "
        "salary_ranks AS ("
        "  SELECT e.id AS employee_id, e.department_id, "
        "    DENSE_RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank "
        "  FROM employees e"
        "), "
        "ranked AS ("
        "  SELECT eh.*, sr.salary_rank, "
        "    ROW_NUMBER() OVER ("
        "      PARTITION BY eh.department_id "
        "      ORDER BY eh.total_hours DESC, eh.salary DESC"
        "    ) AS rn "
        "  FROM employee_hours eh "
        "  JOIN dept_employee_count dc ON dc.department_id = eh.department_id "
        "  JOIN salary_ranks sr ON sr.employee_id = eh.employee_id"
        ") "
        "SELECT d.name AS department_name, r.employee_name, r.total_hours, "
        "  r.num_projects, r.salary_rank "
        "FROM ranked r "
        "JOIN departments d ON d.id = r.department_id "
        "WHERE r.rn = 1 "
        "ORDER BY r.total_hours DESC",
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
    print(f"  HARD TEST: {TASK_ID}")
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

            # Print query formatted for readability
            q_display = query.replace("(", "(\n    ").replace(") ", ")\n") if len(query) > 150 else query
            for ql in q_display.split("\n")[:6]:
                print(f"           {ql.strip()}")
            if len(q_display.split("\n")) > 6:
                print(f"           ... (query truncated)")

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
                for line in lines[:10]:
                    print(f"           {line}")
                if len(lines) > 10:
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
