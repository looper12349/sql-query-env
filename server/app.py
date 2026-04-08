"""FastAPI application for the SQL Query Builder environment."""

import sys
import os

# Ensure the project root is on the path so models.py can be imported
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openenv.core.env_server.http_server import create_app

from models import SqlQueryAction, SqlQueryObservation
from .environment import SqlQueryEnvironment

app = create_app(
    env=SqlQueryEnvironment,
    action_cls=SqlQueryAction,
    observation_cls=SqlQueryObservation,
    env_name="sql_query_env",
)


def main():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
