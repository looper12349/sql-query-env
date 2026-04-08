"""Pydantic models for the SQL Query Builder environment."""

from typing import Dict, Any, List, Optional

from pydantic import Field
from openenv.core.env_server import Action, Observation, State


class SqlQueryAction(Action):
    """Agent sends a SQL query to execute or submit as final answer."""

    action_type: str = Field(
        default="execute",
        description="Either 'execute' (test a query) or 'submit' (final answer).",
    )
    query: str = Field(
        default="",
        description="The SQL query string to execute against the database.",
    )


class SqlQueryObservation(Observation):
    """What the agent sees after each step."""

    task_id: str = Field(default="", description="Current task identifier.")
    task_description: str = Field(
        default="", description="Natural language question the agent must answer."
    )
    schema_description: str = Field(
        default="", description="Database schema (CREATE TABLE statements)."
    )
    query_result: str = Field(
        default="", description="Result rows from the last query, or empty string."
    )
    row_count: int = Field(
        default=0, description="Number of rows returned by the last query."
    )
    column_names: List[str] = Field(
        default_factory=list,
        description="Column names from the last query result.",
    )
    error_message: Optional[str] = Field(
        default=None, description="SQL error message if the query failed."
    )
    steps_remaining: int = Field(
        default=0, description="Number of steps the agent has left."
    )
    hints: Optional[str] = Field(
        default=None,
        description="Optional hint shown after repeated failures.",
    )


class SqlQueryState(State):
    """Internal environment state."""

    task_id: str = Field(default="")
    max_steps: int = Field(default=5)
    queries_executed: List[str] = Field(default_factory=list)
    cumulative_reward: float = Field(default=0.0)
    submitted: bool = Field(default=False)
