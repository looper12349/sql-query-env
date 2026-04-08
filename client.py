"""EnvClient subclass for the SQL Query Builder environment."""

from typing import Any, Dict

from openenv.core.env_client import EnvClient
from openenv.core.client_types import StepResult

from models import SqlQueryAction, SqlQueryObservation, SqlQueryState


class SqlQueryEnvClient(EnvClient[SqlQueryAction, SqlQueryObservation, SqlQueryState]):
    """WebSocket client for the SQL Query Builder environment."""

    def _step_payload(self, action: SqlQueryAction) -> Dict[str, Any]:
        return action.model_dump(exclude={"metadata"})

    def _parse_result(self, payload: Dict[str, Any]) -> StepResult[SqlQueryObservation]:
        obs = SqlQueryObservation(**payload)
        return StepResult(
            observation=obs,
            reward=obs.reward,
            done=obs.done,
        )

    def _parse_state(self, payload: Dict[str, Any]) -> SqlQueryState:
        return SqlQueryState(**payload)


# Convenience aliases matching OpenEnv naming conventions
SqlQueryEnvAction = SqlQueryAction
SqlQueryEnvObservation = SqlQueryObservation
SqlQueryEnvState = SqlQueryState
SqlQueryEnvEnv = SqlQueryEnvClient
