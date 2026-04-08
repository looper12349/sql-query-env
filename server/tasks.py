"""Task definitions and graders for the SQL Query Builder environment.

Each task has:
- task_id: unique identifier
- description: natural language question for the agent
- difficulty: easy / medium / hard
- max_steps: how many steps the agent gets
- expected_query: reference SQL that produces the correct answer
- grader: function(submitted_result, expected_result) -> float in [0, 1]
"""

import sqlite3
from typing import Any

from .database import execute_query


# ---------------------------------------------------------------------------
# Task definitions
# ---------------------------------------------------------------------------

TASKS = {
    "simple_lookup": {
        "task_id": "simple_lookup",
        "difficulty": "easy",
        "max_steps": 5,
        "description": (
            "List all employees in the Engineering department along with their "
            "salary, sorted by salary in descending order. "
            "Return columns: employee_name, salary"
        ),
        "expected_query": """
            SELECT e.name AS employee_name, e.salary
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            WHERE d.name = 'Engineering'
            ORDER BY e.salary DESC
        """,
    },
    "aggregation_analysis": {
        "task_id": "aggregation_analysis",
        "difficulty": "medium",
        "max_steps": 8,
        "description": (
            "For each department that has at least one active project, show: "
            "department_name, employee_count (number of employees in that department), "
            "avg_salary (average salary rounded to 2 decimal places), and "
            "active_project_count (number of active projects). "
            "Only include departments where the average salary exceeds 80000. "
            "Sort by avg_salary descending."
        ),
        "expected_query": """
            SELECT
                d.name AS department_name,
                COUNT(DISTINCT e.id) AS employee_count,
                ROUND(AVG(e.salary), 2) AS avg_salary,
                COUNT(DISTINCT p.id) AS active_project_count
            FROM departments d
            JOIN employees e ON e.department_id = d.id
            JOIN projects p ON p.department_id = d.id AND p.status = 'active'
            GROUP BY d.id, d.name
            HAVING AVG(e.salary) > 80000
            ORDER BY avg_salary DESC
        """,
    },
    "complex_analytics": {
        "task_id": "complex_analytics",
        "difficulty": "hard",
        "max_steps": 12,
        "description": (
            "For each department with at least 2 employees assigned to projects, "
            "find the employee who works the most total hours per week across all "
            "their project assignments. Show: department_name, employee_name, "
            "total_hours (sum of hours_per_week across all assignments), "
            "num_projects (number of distinct projects they are assigned to), "
            "and salary_rank (their rank within their department by salary descending, "
            "using DENSE_RANK). "
            "If two employees in the same department have the same total hours, "
            "pick the one with the higher salary. "
            "Sort the final result by total_hours descending."
        ),
        "expected_query": """
            WITH employee_hours AS (
                SELECT
                    e.id AS employee_id,
                    e.name AS employee_name,
                    e.department_id,
                    e.salary,
                    SUM(a.hours_per_week) AS total_hours,
                    COUNT(DISTINCT a.project_id) AS num_projects
                FROM employees e
                JOIN assignments a ON a.employee_id = e.id
                GROUP BY e.id, e.name, e.department_id, e.salary
            ),
            dept_employee_count AS (
                SELECT department_id, COUNT(DISTINCT employee_id) AS emp_count
                FROM assignments a
                JOIN employees e ON e.id = a.employee_id
                GROUP BY department_id
                HAVING COUNT(DISTINCT employee_id) >= 2
            ),
            salary_ranks AS (
                SELECT
                    e.id AS employee_id,
                    e.department_id,
                    DENSE_RANK() OVER (
                        PARTITION BY e.department_id ORDER BY e.salary DESC
                    ) AS salary_rank
                FROM employees e
            ),
            ranked AS (
                SELECT
                    eh.*,
                    sr.salary_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY eh.department_id
                        ORDER BY eh.total_hours DESC, eh.salary DESC
                    ) AS rn
                FROM employee_hours eh
                JOIN dept_employee_count dc ON dc.department_id = eh.department_id
                JOIN salary_ranks sr ON sr.employee_id = eh.employee_id
            )
            SELECT
                d.name AS department_name,
                r.employee_name,
                r.total_hours,
                r.num_projects,
                r.salary_rank
            FROM ranked r
            JOIN departments d ON d.id = r.department_id
            WHERE r.rn = 1
            ORDER BY r.total_hours DESC
        """,
    },
}


# ---------------------------------------------------------------------------
# Grading helpers
# ---------------------------------------------------------------------------


def _normalize_value(val: Any) -> Any:
    """Normalize a value for comparison."""
    if val is None:
        return None
    if isinstance(val, float):
        return round(val, 2)
    if isinstance(val, str):
        return val.strip().lower()
    return val


def _normalize_row(row: tuple) -> tuple:
    return tuple(_normalize_value(v) for v in row)


def _format_rows(rows: list, columns: list) -> str:
    """Format rows for display to the agent."""
    if not rows:
        return "(no rows)"
    lines = [" | ".join(str(c) for c in columns)]
    lines.append("-" * len(lines[0]))
    for row in rows[:50]:  # Limit display to 50 rows
        lines.append(" | ".join(str(v) for v in row))
    if len(rows) > 50:
        lines.append(f"... ({len(rows)} total rows)")
    return "\n".join(lines)


def get_expected_result(conn: sqlite3.Connection, task_id: str) -> dict:
    """Run the expected query for a task and return the result."""
    task = TASKS[task_id]
    return execute_query(conn, task["expected_query"])


# ---------------------------------------------------------------------------
# Step-level reward (partial credit during exploration)
# ---------------------------------------------------------------------------


def compute_step_reward(
    conn: sqlite3.Connection,
    task_id: str,
    query: str,
    query_result: dict,
    previous_queries: list[str],
) -> float:
    """Compute partial reward for an 'execute' action. Always returns [0.0, 1.0]."""
    reward = 0.0

    # No reward for repeated exact same query
    if query.strip() in [q.strip() for q in previous_queries]:
        return 0.0

    # Reduce reward for SELECT * (lazy) — still get something for trying
    select_star = "select *" in query.lower().replace("\n", " ")

    if not query_result["success"]:
        # Small reward for attempting a query
        return 0.05

    # Query executed successfully
    reward += 0.15

    # Penalize SELECT * by giving less base reward
    if select_star:
        reward = 0.10

    expected = get_expected_result(conn, task_id)
    if not expected["success"]:
        return round(min(reward, 1.0), 2)

    # Correct number of columns
    if len(query_result["columns"]) == len(expected["columns"]):
        reward += 0.10

    # Correct number of rows
    if query_result["row_count"] == expected["row_count"]:
        reward += 0.10

    # Partial row overlap
    expected_rows = {_normalize_row(r) for r in expected["rows"]}
    submitted_rows = {_normalize_row(r) for r in query_result["rows"]}
    if expected_rows:
        overlap = len(expected_rows & submitted_rows) / len(expected_rows)
        reward += overlap * 0.15

    return round(min(max(reward, 0.0), 1.0), 2)


# ---------------------------------------------------------------------------
# Final grader (submit action)
# ---------------------------------------------------------------------------


def grade_submission(
    conn: sqlite3.Connection,
    task_id: str,
    submitted_result: dict,
) -> float:
    """Grade a submitted query. Always returns score in [0.0, 1.0]."""
    if not submitted_result["success"]:
        return 0.1  # At least they submitted something

    expected = get_expected_result(conn, task_id)
    if not expected["success"]:
        return 0.0  # Should never happen

    score = 0.0

    # 1) Query executes without error: +0.15
    score += 0.15

    # 2) Correct column names: +0.15
    expected_cols = [c.lower().strip() for c in expected["columns"]]
    submitted_cols = [c.lower().strip() for c in submitted_result["columns"]]
    if expected_cols == submitted_cols:
        score += 0.15
    elif set(expected_cols) == set(submitted_cols):
        # Columns present but wrong order
        score += 0.10

    # 3) Correct number of rows: +0.20
    if submitted_result["row_count"] == expected["row_count"]:
        score += 0.20
    elif abs(submitted_result["row_count"] - expected["row_count"]) <= 1:
        score += 0.10  # Off by one

    # 4) Exact row content match (order-insensitive): +0.30
    expected_rows_set = {_normalize_row(r) for r in expected["rows"]}
    submitted_rows_set = {_normalize_row(r) for r in submitted_result["rows"]}
    if expected_rows_set == submitted_rows_set:
        score += 0.30
    elif expected_rows_set:
        overlap = len(expected_rows_set & submitted_rows_set) / len(expected_rows_set)
        score += round(overlap * 0.20, 2)  # Partial credit

    # 5) Correct ordering: +0.20
    expected_rows_ordered = [_normalize_row(r) for r in expected["rows"]]
    submitted_rows_ordered = [_normalize_row(r) for r in submitted_result["rows"]]
    if expected_rows_ordered == submitted_rows_ordered:
        score += 0.20
    elif expected_rows_set == submitted_rows_set:
        # Right data, wrong order — partial credit
        score += 0.10

    return round(min(max(score, 0.0), 1.0), 2)
