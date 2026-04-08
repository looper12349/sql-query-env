"""
Inference Script — SQL Query Builder Environment
===================================
MANDATORY
- Before submitting, ensure the following variables are defined in your environment configuration:
    API_BASE_URL   The API endpoint for the LLM.
    MODEL_NAME     The model identifier to use for inference.
    HF_TOKEN       Your Hugging Face / API key.
    LOCAL_IMAGE_NAME The name of the local image to use for the environment if you are using from_docker_image()

STDOUT FORMAT
- The script must emit exactly three line types to stdout, in this order:

    [START] task=<task_name> env=<benchmark> model=<model_name>
    [STEP]  step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
    [END]   success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>
"""

import asyncio
import os
import textwrap
from typing import List, Optional

from openai import OpenAI

from client import SqlQueryEnvClient, SqlQueryAction

LOCAL_IMAGE_NAME = os.getenv("LOCAL_IMAGE_NAME")
HF_TOKEN = os.getenv("HF_TOKEN")

API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
BENCHMARK = "sql_query_env"
MAX_TEMPERATURE = 0.3
MAX_TOKENS = 512

TASKS_TO_RUN = ["simple_lookup", "aggregation_analysis", "complex_analytics"]

SYSTEM_PROMPT = textwrap.dedent("""
    You are an expert SQL query builder. You are given a database schema and a
    natural language question. Your goal is to write a correct SQL query that
    answers the question.

    On each turn you will see:
    - The database schema (CREATE TABLE statements)
    - The question you need to answer
    - The result of your last query (or error message)
    - How many steps you have remaining

    You must respond with EXACTLY a JSON object in this format:
    {"action_type": "execute", "query": "YOUR SQL QUERY HERE"}

    Use "execute" to test a query and see results.
    Use "submit" when you are confident your query is correct.

    Rules:
    - Only use SELECT or WITH (CTE) statements
    - Pay close attention to exact column names in the schema
    - Check your results before submitting
    - If you get an error, read it carefully and fix the issue
    - Do NOT use SELECT * — always specify columns explicitly
""").strip()


def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    # Truncate action for logging
    action_short = action.replace("\n", " ")[:200]
    print(
        f"[STEP] step={step} action={action_short} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


def build_user_prompt(
    schema: str,
    question: str,
    last_result: str,
    last_error: Optional[str],
    steps_remaining: int,
    history: List[str],
) -> str:
    history_block = "\n".join(history[-4:]) if history else "None"
    error_block = f"\nLast error: {last_error}" if last_error else ""
    return textwrap.dedent(f"""
        Database Schema:
        {schema}

        Question: {question}

        Last query result:
        {last_result if last_result else '(no query executed yet)'}
        {error_block}

        Steps remaining: {steps_remaining}

        Previous attempts:
        {history_block}

        Respond with a JSON object: {{"action_type": "execute"|"submit", "query": "..."}}
    """).strip()


def parse_model_response(text: str) -> tuple:
    """Parse the model's JSON response into (action_type, query)."""
    import json
    import re

    text = text.strip()

    # Try to extract JSON from the response
    # Handle markdown code blocks
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if json_match:
        text = json_match.group(1)
    else:
        # Try to find raw JSON
        json_match = re.search(r'\{[^{}]*"action_type"[^{}]*\}', text, re.DOTALL)
        if json_match:
            text = json_match.group(0)

    try:
        data = json.loads(text)
        action_type = data.get("action_type", "execute")
        query = data.get("query", "")
        return action_type, query
    except json.JSONDecodeError:
        # Fallback: try to extract SQL directly
        sql_match = re.search(r'(SELECT|WITH)\s+.+', text, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return "execute", sql_match.group(0).strip()
        return "execute", "SELECT 1"


def get_model_response(
    client: OpenAI,
    schema: str,
    question: str,
    last_result: str,
    last_error: Optional[str],
    steps_remaining: int,
    history: List[str],
) -> tuple:
    """Call the LLM and return (action_type, query)."""
    user_prompt = build_user_prompt(
        schema, question, last_result, last_error, steps_remaining, history,
    )
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=MAX_TEMPERATURE,
            max_tokens=MAX_TOKENS,
            stream=False,
        )
        text = (completion.choices[0].message.content or "").strip()
        return parse_model_response(text)
    except Exception as exc:
        print(f"[DEBUG] Model request failed: {exc}", flush=True)
        return "execute", "SELECT 1"


async def run_task(client: OpenAI, env: SqlQueryEnvClient, task_id: str) -> float:
    """Run a single task and return the final score."""
    history: List[str] = []
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False

    log_start(task=task_id, env=BENCHMARK, model=MODEL_NAME)

    try:
        result = await env.reset(task_id=task_id)
        obs = result.observation
        last_result = obs.query_result
        last_error = obs.error_message
        schema = obs.schema_description
        question = obs.task_description
        steps_remaining = obs.steps_remaining

        max_steps = steps_remaining

        for step in range(1, max_steps + 1):
            if result.done:
                break

            action_type, query = get_model_response(
                client, schema, question, last_result, last_error,
                steps_remaining, history,
            )

            # On last step, force submit
            if step == max_steps and action_type != "submit":
                action_type = "submit"

            action = SqlQueryAction(action_type=action_type, query=query)
            result = await env.step(action)
            obs = result.observation

            reward = result.reward or 0.0
            done = result.done
            error = obs.error_message

            rewards.append(reward)
            steps_taken = step
            last_result = obs.query_result
            last_error = obs.error_message
            steps_remaining = obs.steps_remaining

            log_step(step=step, action=query, reward=reward, done=done, error=error)

            history.append(
                f"Step {step} ({action_type}): {query[:100]}... -> "
                f"reward={reward:+.2f}, error={error or 'none'}"
            )

            if done:
                break

        # Final score is the last reward on submit, clamped to [0, 1]
        if rewards:
            score = max(rewards[-1], 0.0)  # The submit/final step reward
            score = min(max(score, 0.0), 1.0)
        success = score >= 0.5

    except Exception as exc:
        print(f"[DEBUG] Task {task_id} error: {exc}", flush=True)
    finally:
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)

    return score


async def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)
    env = await SqlQueryEnvClient.from_docker_image(LOCAL_IMAGE_NAME)

    scores = {}
    try:
        for task_id in TASKS_TO_RUN:
            score = await run_task(client, env, task_id)
            scores[task_id] = score
            print(f"[DEBUG] Task {task_id} score: {score:.2f}", flush=True)
    finally:
        try:
            await env.close()
        except Exception as e:
            print(f"[DEBUG] env.close() error: {e}", flush=True)

    print("\n=== Final Scores ===", flush=True)
    for task_id, score in scores.items():
        print(f"  {task_id}: {score:.2f}", flush=True)
    avg = sum(scores.values()) / len(scores) if scores else 0.0
    print(f"  Average: {avg:.2f}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
