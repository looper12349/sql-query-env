"""SQLite in-memory database with schema and seed data."""

import sqlite3

SCHEMA_SQL = """
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    budget REAL NOT NULL,
    location TEXT NOT NULL
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    hire_date TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    salary REAL NOT NULL,
    manager_id INTEGER,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (manager_id) REFERENCES employees(id)
);

CREATE TABLE projects (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT,
    status TEXT NOT NULL CHECK(status IN ('active', 'completed', 'cancelled')),
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE assignments (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    role TEXT NOT NULL,
    hours_per_week REAL NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id),
    FOREIGN KEY (employee_id) REFERENCES employees(id)
);
"""

SEED_SQL = """
-- Departments
INSERT INTO departments VALUES (1, 'Engineering', 500000, 'San Francisco');
INSERT INTO departments VALUES (2, 'Marketing', 200000, 'New York');
INSERT INTO departments VALUES (3, 'Sales', 300000, 'Chicago');
INSERT INTO departments VALUES (4, 'HR', 150000, 'San Francisco');
INSERT INTO departments VALUES (5, 'Research', 400000, 'Boston');

-- Employees (30 total, some tricky data)
-- Engineering (dept 1) - 8 employees
INSERT INTO employees VALUES (1, 'Alice Chen', 1, '2019-03-15', 'alice@company.com', 145000, NULL);
INSERT INTO employees VALUES (2, 'Bob Kumar', 1, '2020-06-01', 'bob@company.com', 120000, 1);
INSERT INTO employees VALUES (3, 'Carol Smith', 1, '2021-01-10', 'carol@company.com', 110000, 1);
INSERT INTO employees VALUES (4, 'David Park', 1, '2021-08-20', 'david@company.com', 95000, 2);
INSERT INTO employees VALUES (5, 'Eva Martinez', 1, '2022-02-14', 'eva@company.com', 105000, 2);
INSERT INTO employees VALUES (6, 'Frank Liu', 1, '2020-11-30', 'frank@company.com', 120000, 1);
INSERT INTO employees VALUES (7, 'Grace Kim', 1, '2023-04-01', 'grace@company.com', 88000, 3);
INSERT INTO employees VALUES (8, 'Henry Zhang', 1, '2019-07-22', 'henry@company.com', 135000, 1);

-- Marketing (dept 2) - 6 employees
INSERT INTO employees VALUES (9, 'Iris Johnson', 2, '2018-09-05', 'iris@company.com', 98000, NULL);
INSERT INTO employees VALUES (10, 'Jack Brown', 2, '2020-03-12', 'jack@company.com', 82000, 9);
INSERT INTO employees VALUES (11, 'Karen White', 2, '2021-06-18', 'karen@company.com', 75000, 9);
INSERT INTO employees VALUES (12, 'Leo Garcia', 2, '2022-01-25', 'leo@company.com', 70000, 10);
INSERT INTO employees VALUES (13, 'Maria Lopez', 2, '2019-11-11', 'maria@company.com', 90000, 9);
INSERT INTO employees VALUES (14, 'Nate Wilson', 2, '2023-02-28', 'nate@company.com', 68000, 10);

-- Sales (dept 3) - 7 employees
INSERT INTO employees VALUES (15, 'Olivia Taylor', 3, '2017-05-20', 'olivia@company.com', 110000, NULL);
INSERT INTO employees VALUES (16, 'Paul Anderson', 3, '2019-08-14', 'paul@company.com', 92000, 15);
INSERT INTO employees VALUES (17, 'Quinn Davis', 3, '2020-12-01', 'quinn@company.com', 85000, 15);
INSERT INTO employees VALUES (18, 'Rachel Moore', 3, '2021-04-07', 'rachel@company.com', 78000, 16);
INSERT INTO employees VALUES (19, 'Sam Jackson', 3, '2022-09-15', 'sam@company.com', 72000, 16);
INSERT INTO employees VALUES (20, 'Tina Harris', 3, '2020-07-30', 'tina@company.com', 88000, 15);
INSERT INTO employees VALUES (21, 'Uma Clark', 3, '2023-01-10', 'uma@company.com', 65000, 17);

-- HR (dept 4) - 4 employees
INSERT INTO employees VALUES (22, 'Victor Lee', 4, '2018-02-15', 'victor@company.com', 95000, NULL);
INSERT INTO employees VALUES (23, 'Wendy Adams', 4, '2020-10-08', 'wendy@company.com', 78000, 22);
INSERT INTO employees VALUES (24, 'Xavier Diaz', 4, '2022-05-22', 'xavier@company.com', 72000, 22);
INSERT INTO employees VALUES (25, 'Yuki Tanaka', 4, '2021-09-01', 'yuki@company.com', 80000, 22);

-- Research (dept 5) - 5 employees
INSERT INTO employees VALUES (26, 'Zara Patel', 5, '2019-01-20', 'zara@company.com', 130000, NULL);
INSERT INTO employees VALUES (27, 'Aaron Brooks', 5, '2020-04-15', 'aaron@company.com', 115000, 26);
INSERT INTO employees VALUES (28, 'Bella Rivera', 5, '2021-07-12', 'bella@company.com', 105000, 26);
INSERT INTO employees VALUES (29, 'Chris Evans', 5, '2022-03-08', 'chris@company.com', 98000, 27);
INSERT INTO employees VALUES (30, 'Diana Foster', 5, '2023-06-01', 'diana@company.com', 92000, 27);

-- Projects (8 total, mixed statuses, some with NULL end_date)
INSERT INTO projects VALUES (1, 'Cloud Migration', 1, '2023-01-01', '2023-12-31', 'completed');
INSERT INTO projects VALUES (2, 'API Redesign', 1, '2024-01-15', NULL, 'active');
INSERT INTO projects VALUES (3, 'Brand Refresh', 2, '2023-06-01', '2024-03-01', 'completed');
INSERT INTO projects VALUES (4, 'Social Media Campaign', 2, '2024-02-01', NULL, 'active');
INSERT INTO projects VALUES (5, 'Q4 Sales Push', 3, '2023-10-01', '2024-01-31', 'completed');
INSERT INTO projects VALUES (6, 'Enterprise Expansion', 3, '2024-03-01', NULL, 'active');
INSERT INTO projects VALUES (7, 'ML Research Platform', 5, '2024-01-01', NULL, 'active');
INSERT INTO projects VALUES (8, 'Legacy System Sunset', 1, '2023-03-01', '2023-09-30', 'cancelled');

-- Assignments (40 total - employees can be on multiple projects)
-- Cloud Migration (project 1)
INSERT INTO assignments VALUES (1, 1, 1, 'tech_lead', 20);
INSERT INTO assignments VALUES (2, 1, 2, 'developer', 30);
INSERT INTO assignments VALUES (3, 1, 3, 'developer', 25);
INSERT INTO assignments VALUES (4, 1, 8, 'architect', 15);
INSERT INTO assignments VALUES (5, 1, 4, 'developer', 35);

-- API Redesign (project 2)
INSERT INTO assignments VALUES (6, 2, 1, 'tech_lead', 15);
INSERT INTO assignments VALUES (7, 2, 5, 'developer', 30);
INSERT INTO assignments VALUES (8, 2, 6, 'developer', 25);
INSERT INTO assignments VALUES (9, 2, 7, 'developer', 40);
INSERT INTO assignments VALUES (10, 2, 3, 'reviewer', 10);
INSERT INTO assignments VALUES (11, 2, 8, 'architect', 20);

-- Brand Refresh (project 3)
INSERT INTO assignments VALUES (12, 3, 9, 'lead', 20);
INSERT INTO assignments VALUES (13, 3, 10, 'designer', 35);
INSERT INTO assignments VALUES (14, 3, 11, 'copywriter', 30);
INSERT INTO assignments VALUES (15, 3, 13, 'strategist', 25);

-- Social Media Campaign (project 4)
INSERT INTO assignments VALUES (16, 4, 9, 'lead', 15);
INSERT INTO assignments VALUES (17, 4, 12, 'content_creator', 35);
INSERT INTO assignments VALUES (18, 4, 14, 'analyst', 25);
INSERT INTO assignments VALUES (19, 4, 11, 'copywriter', 20);
INSERT INTO assignments VALUES (20, 4, 13, 'strategist', 15);

-- Q4 Sales Push (project 5)
INSERT INTO assignments VALUES (21, 5, 15, 'lead', 15);
INSERT INTO assignments VALUES (22, 5, 16, 'account_manager', 30);
INSERT INTO assignments VALUES (23, 5, 17, 'account_manager', 30);
INSERT INTO assignments VALUES (24, 5, 18, 'support', 25);
INSERT INTO assignments VALUES (25, 5, 20, 'analyst', 20);

-- Enterprise Expansion (project 6)
INSERT INTO assignments VALUES (26, 6, 15, 'lead', 20);
INSERT INTO assignments VALUES (27, 6, 16, 'account_manager', 25);
INSERT INTO assignments VALUES (28, 6, 19, 'account_manager', 35);
INSERT INTO assignments VALUES (29, 6, 20, 'analyst', 25);
INSERT INTO assignments VALUES (30, 6, 21, 'support', 30);
INSERT INTO assignments VALUES (31, 6, 17, 'account_manager', 15);

-- ML Research Platform (project 7)
INSERT INTO assignments VALUES (32, 7, 26, 'principal_researcher', 25);
INSERT INTO assignments VALUES (33, 7, 27, 'researcher', 35);
INSERT INTO assignments VALUES (34, 7, 28, 'researcher', 30);
INSERT INTO assignments VALUES (35, 7, 29, 'engineer', 40);
INSERT INTO assignments VALUES (36, 7, 30, 'engineer', 35);
INSERT INTO assignments VALUES (37, 7, 2, 'consultant', 10);

-- Legacy System Sunset (project 8 - cancelled)
INSERT INTO assignments VALUES (38, 8, 8, 'lead', 10);
INSERT INTO assignments VALUES (39, 8, 4, 'developer', 20);
INSERT INTO assignments VALUES (40, 8, 6, 'developer', 15);
"""

SCHEMA_DESCRIPTION = """Database Schema:
--------------
TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    budget REAL NOT NULL,
    location TEXT NOT NULL
);

TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER NOT NULL REFERENCES departments(id),
    hire_date TEXT NOT NULL,       -- format: 'YYYY-MM-DD'
    email TEXT NOT NULL UNIQUE,
    salary REAL NOT NULL,
    manager_id INTEGER REFERENCES employees(id)  -- NULL for top-level managers
);

TABLE projects (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER NOT NULL REFERENCES departments(id),
    start_date TEXT NOT NULL,
    end_date TEXT,                 -- NULL means ongoing
    status TEXT NOT NULL           -- 'active', 'completed', or 'cancelled'
);

TABLE assignments (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    employee_id INTEGER NOT NULL REFERENCES employees(id),
    role TEXT NOT NULL,
    hours_per_week REAL NOT NULL
);

Notes:
- 5 departments, 30 employees, 8 projects, 40 assignments
- Some employees have manager_id = NULL (top-level managers)
- Some projects have end_date = NULL (ongoing)
- Employees can be assigned to multiple projects
- Projects can have employees from different departments (cross-functional)
"""


def create_database() -> sqlite3.Connection:
    """Create a fresh in-memory SQLite database with schema and seed data."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL)
    conn.executescript(SEED_SQL)
    return conn


def execute_query(conn: sqlite3.Connection, query: str) -> dict:
    """Execute a SQL query and return results or error.

    Returns dict with keys:
        success: bool
        columns: list[str] (if success)
        rows: list[tuple] (if success)
        row_count: int (if success)
        error: str (if not success)
    """
    query = query.strip()
    if not query:
        return {"success": False, "columns": [], "rows": [], "row_count": 0, "error": "Empty query"}

    # Only allow SELECT statements
    first_word = query.split()[0].upper() if query.split() else ""
    if first_word not in ("SELECT", "WITH"):
        return {
            "success": False,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "error": "Only SELECT and WITH (CTE) queries are allowed. "
                     "You cannot modify the database.",
        }

    try:
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return {
            "success": True,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "error": None,
        }
    except Exception as e:
        return {
            "success": False,
            "columns": [],
            "rows": [],
            "row_count": 0,
            "error": str(e),
        }
