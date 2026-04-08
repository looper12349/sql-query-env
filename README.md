---
title: SQL Query Builder
emoji: 🗃️
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 8000
tags:
  - openenv
---

# SQL Query Builder — OpenEnv Environment

An OpenEnv environment where an AI agent iteratively writes and debugs SQL queries against a company database to answer natural language questions.

## Motivation

Writing correct SQL is a core skill for data professionals, yet LLMs frequently hallucinate column names, produce wrong JOINs, and fail on complex aggregations. This environment tests an agent's ability to:

- Read a schema and reason about table relationships
- Write syntactically and semantically correct SQL
- Interpret error messages and query results to iteratively refine
- Handle multi-step reasoning (CTEs, window functions, subqueries)

## Action Space

| Field | Type | Description |
|-------|------|-------------|
| `action_type` | `"execute"` or `"submit"` | Execute tests a query; submit grades it as final answer |
| `query` | `str` | SQL query (SELECT/WITH only) |

## Observation Space

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | `str` | Current task identifier |
| `task_description` | `str` | Natural language question |
| `schema_description` | `str` | Database schema (DDL) |
| `query_result` | `str` | Formatted result rows or error |
| `row_count` | `int` | Rows returned |
| `column_names` | `list[str]` | Column names from result |
| `error_message` | `str \| null` | SQL error if query failed |
| `steps_remaining` | `int` | Steps left in the episode |
| `hints` | `str \| null` | Hint after repeated failures |

## Tasks

| Task ID | Difficulty | Max Steps | Description |
|---------|-----------|-----------|-------------|
| `simple_lookup` | Easy | 5 | Single JOIN + WHERE + ORDER BY |
| `aggregation_analysis` | Medium | 8 | Multi-JOIN + GROUP BY + HAVING + filtering |
| `complex_analytics` | Hard | 12 | CTEs + window functions + tiebreakers |

### Expected Difficulty

- **Easy**: Agents score 0.7–1.0. Simple schema navigation, recoverable from column name errors.
- **Medium**: Agents score 0.3–0.6. Multiple JOINs cause row duplication, agents confuse related columns.
- **Hard**: Agents score 0.05–0.3. Window functions in SQLite trip most models, tiebreaker logic is subtle.

## Database Schema

- **departments** (5 rows): id, name, budget, location
- **employees** (30 rows): id, name, department_id, hire_date, email, salary, manager_id
- **projects** (8 rows): id, name, department_id, start_date, end_date, status
- **assignments** (40 rows): id, project_id, employee_id, role, hours_per_week

Tricky data: NULL manager_ids, NULL end_dates, duplicate salaries, empty departments, cross-department assignments.

## Reward Design

**Execute steps** (partial credit):
- Valid SQL execution: +0.10
- Correct column count: +0.05
- Correct row count: +0.05
- Partial row match: up to +0.10
- Penalty for repeated queries: -0.05
- Penalty for SELECT *: -0.03

**Submit step** (final grading):
- Executes without error: +0.15
- Correct column names: +0.15
- Correct row count: +0.20
- Exact row content: +0.30
- Correct ordering: +0.20

## Setup

```bash
# Install dependencies
uv sync

# Run the server locally
uv run python -m server.app

# Run with Docker
docker build -t sql-query-env .
docker run -p 8000:8000 sql-query-env

# Validate
openenv validate
```

## Baseline Inference

```bash
export HF_TOKEN=your_token
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
export IMAGE_NAME=sql-query-env

python inference.py
```

## Baseline Scores

| Task | Expected Score |
|------|---------------|
| simple_lookup | 0.70 – 1.00 |
| aggregation_analysis | 0.30 – 0.60 |
| complex_analytics | 0.05 – 0.30 |
