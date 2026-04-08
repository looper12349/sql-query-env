"""SQL Query Builder Environment — core logic."""

import uuid
from typing import Any, Optional

from openenv.core.env_server import Environment

from models import SqlQueryAction, SqlQueryObservation, SqlQueryState
from .database import create_database, execute_query, SCHEMA_DESCRIPTION
from .tasks import TASKS, compute_step_reward, grade_submission, get_expected_result, _format_rows


class SqlQueryEnvironment(Environment[SqlQueryAction, SqlQueryObservation, SqlQueryState]):
    """An environment where an AI agent iteratively builds SQL queries."""

    SUPPORTS_CONCURRENT_SESSIONS = False

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._conn = None
        self._state = SqlQueryState()
        self._task = None
        self._last_observation = None

    def get_metadata(self):
        from openenv.core.env_server.types import EnvironmentMetadata
        return EnvironmentMetadata(
            name="sql_query_env",
            description=(
                "SQL Query Builder: an agent iteratively writes and debugs SQL queries "
                "against a company database to answer natural language questions."
            ),
        )

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        **kwargs: Any,
    ) -> SqlQueryObservation:
        # Determine which task to load
        task_id = kwargs.get("task_id", "simple_lookup")
        if task_id not in TASKS:
            task_id = "simple_lookup"

        self._task = TASKS[task_id]

        # Fresh database for each episode
        if self._conn:
            self._conn.close()
        self._conn = create_database()

        # Reset state
        self._state = SqlQueryState(
            episode_id=episode_id or str(uuid.uuid4()),
            step_count=0,
            task_id=task_id,
            max_steps=self._task["max_steps"],
            queries_executed=[],
            cumulative_reward=0.0,
            submitted=False,
        )

        obs = SqlQueryObservation(
            task_id=task_id,
            task_description=self._task["description"],
            schema_description=SCHEMA_DESCRIPTION,
            query_result="",
            row_count=0,
            column_names=[],
            error_message=None,
            steps_remaining=self._task["max_steps"],
            done=False,
            reward=0.0,
        )
        self._last_observation = obs
        return obs

    def step(
        self,
        action: SqlQueryAction,
        timeout_s: Optional[float] = None,
        **kwargs: Any,
    ) -> SqlQueryObservation:
        if self._task is None:
            return SqlQueryObservation(
                error_message="Environment not initialized. Call reset() first.",
                done=True,
                reward=0.0,
            )

        self._state.step_count += 1
        steps_remaining = self._state.max_steps - self._state.step_count

        action_type = (action.action_type or "execute").lower().strip()
        query = (action.query or "").strip()

        # Handle empty query
        if not query:
            return SqlQueryObservation(
                task_id=self._state.task_id,
                task_description=self._task["description"],
                schema_description=SCHEMA_DESCRIPTION,
                query_result="",
                row_count=0,
                column_names=[],
                error_message="No query provided. Please enter a SQL query.",
                steps_remaining=max(steps_remaining, 0),
                done=steps_remaining <= 0,
                reward=0.0,
            )

        if action_type == "submit":
            # Final submission — grade it
            result = execute_query(self._conn, query)
            score = grade_submission(self._conn, self._state.task_id, result)
            score = round(min(max(score, 0.0), 1.0), 2)
            self._state.submitted = True
            self._state.queries_executed.append(query)
            self._state.cumulative_reward = round(min(self._state.cumulative_reward + score, 1.0), 2)

            # Format result for display
            result_str = ""
            if result["success"]:
                result_str = _format_rows(result["rows"], result["columns"])
            else:
                result_str = f"ERROR: {result['error']}"

            obs = SqlQueryObservation(
                task_id=self._state.task_id,
                task_description=self._task["description"],
                schema_description=SCHEMA_DESCRIPTION,
                query_result=result_str,
                row_count=result.get("row_count", 0),
                column_names=result.get("columns", []),
                error_message=result.get("error"),
                steps_remaining=0,
                done=True,
                reward=score,
            )
            self._last_observation = obs
            return obs

        elif action_type == "execute":
            # Exploratory query — partial reward
            result = execute_query(self._conn, query)
            reward = compute_step_reward(
                self._conn,
                self._state.task_id,
                query,
                result,
                self._state.queries_executed,
            )
            reward = round(min(max(reward, 0.0), 1.0), 2)
            self._state.queries_executed.append(query)
            self._state.cumulative_reward = round(min(self._state.cumulative_reward + reward, 1.0), 2)

            # Format result for display
            result_str = ""
            if result["success"]:
                result_str = _format_rows(result["rows"], result["columns"])
            else:
                result_str = f"ERROR: {result['error']}"

            # Check if steps exhausted
            done = steps_remaining <= 0
            if done:
                # Auto-grade last query if steps ran out
                if result["success"]:
                    final_score = grade_submission(
                        self._conn, self._state.task_id, result
                    )
                    reward = round(min(max(final_score, 0.0), 1.0), 2)
                else:
                    reward = 0.0

            # Provide hint after repeated failures
            hint = None
            error_count = sum(
                1
                for q in self._state.queries_executed
                if not execute_query(self._conn, q)["success"]
            )
            if error_count >= 3:
                hint = (
                    "Hint: Check column names carefully. Use the schema description "
                    "to verify table and column names before writing your query."
                )

            obs = SqlQueryObservation(
                task_id=self._state.task_id,
                task_description=self._task["description"],
                schema_description=SCHEMA_DESCRIPTION,
                query_result=result_str,
                row_count=result.get("row_count", 0),
                column_names=result.get("columns", []),
                error_message=result.get("error"),
                steps_remaining=max(steps_remaining, 0),
                hints=hint,
                done=done,
                reward=reward,
            )
            self._last_observation = obs
            return obs

        else:
            # Invalid action type — zero reward
            obs = SqlQueryObservation(
                task_id=self._state.task_id,
                task_description=self._task["description"],
                schema_description=SCHEMA_DESCRIPTION,
                query_result="",
                row_count=0,
                column_names=[],
                error_message=f"Invalid action_type '{action_type}'. Use 'execute' or 'submit'.",
                steps_remaining=max(steps_remaining, 0),
                done=steps_remaining <= 0,
                reward=0.0,
            )
            self._last_observation = obs
            return obs

    @property
    def state(self) -> SqlQueryState:
        return self._state

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
