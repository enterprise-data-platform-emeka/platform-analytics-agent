# platform-analytics-agent

This is the Natural Language (NL) Analytics Agent for the Enterprise Data Platform. It's the final layer of the platform: everything before this (DMS (Database Migration Service), Glue, dbt, MWAA (Amazon Managed Workflows for Apache Airflow)) exists to produce a clean, curated Gold data layer. This agent makes that data accessible to anyone who can ask a question in plain English, without needing to know SQL, table names, or partition structures.

---

## What problem this solves

The Gold layer holds carefully curated, business-ready aggregations. Getting value from it still requires an analyst who can write Athena SQL, knows the exact table and column names, and understands the partition structure well enough not to run expensive full-table scans. Most people at a company can't do all three. This agent removes that barrier.

A user asks: "Show me monthly transaction volume for Berlin over the last 12 months."

The agent:
1. Identifies the correct Gold table from the dbt (data build tool) schema catalog
2. Reads the partition keys from the Glue Data Catalog (Glue Catalog)
3. Generates an Athena SQL query with a partition filter to minimise scan cost
4. Checks the estimated bytes scanned before executing
5. Runs the query and validates the result for obvious anomalies
6. Produces a time-series chart
7. Returns a plain-English insight alongside the SQL it ran and every assumption it made

If it interpreted "transactions" as completed orders only, it says so explicitly before returning the result, so the user can catch that interpretation and correct it.

---

## Why Athena specifically

Amazon Athena is a serverless SQL (Structured Query Language) query engine that runs directly over S3 (Simple Storage Service) data. The cost model is pay-per-byte-scanned, not per compute hour. A query that scans the whole table because a partition filter is missing doesn't just run slowly — it costs real money and could easily hit the WorkGroup scan limit.

This is different from Databricks, BigQuery (Google's managed data warehouse), or Snowflake, which are managed warehouses with internal storage. Those platforms already have built-in NL (Natural Language) query features. Athena doesn't. Nobody ships an NL-to-SQL product that reasons about S3 partition structures and Glue Catalog metadata for cost optimisation. That's what this agent does.

The schema context is also richer here than in most text-to-SQL systems. The agent reads from two sources simultaneously:

- **Glue Catalog (live):** column names, data types, partition keys. Always current.
- **dbt catalog.json (from S3):** column descriptions, model documentation, accepted values, lineage. Written after every successful pipeline run by the MWAA DAG's `upload_dbt_artifacts` task.

Most NL-to-SQL tools only see column names. This agent sees the business meaning behind every column.

---

## Architecture

```mermaid
flowchart TD
    subgraph Startup ["Startup - once per ECS task"]
        LoadSchemas["load_all_schemas()\nGlue Catalog + dbt catalog.json\nAll 7 Gold schemas into system prompt"]
    end

    subgraph Input ["Input"]
        User([User NL Question\ne.g. via CLI or HTTP])
    end

    subgraph AgentLoop ["Agent Reasoning Loop - ECS Fargate"]
        direction TB
        GenerateSQL["Generate SQL + Assumptions\nClaude API - single call\nSchema already in system prompt"]
        ValidateSQL["validate_sql()\nsqlparse guardrails"]
        Execute["execute_query()\nAthena SDK"]
        TrackCost["cost.py\nDataScannedInBytes to USD"]
        ValidateResults["validate_results()\nsanity checks"]
        RenderChart["render_chart()\nmatplotlib + Plotly"]
        Summarise[Generate Insight\nClaude API]
    end

    subgraph AWS ["AWS Data Platform"]
        GlueCatalog[(Glue Catalog\nLive Schema)]
        DbtArtifacts[(dbt catalog.json\nS3 Metadata)]
        Athena[(Athena\nedp dev gold)]
        S3Gold[(S3 Gold Layer\nParquet)]
        AuditLog[(S3 Audit Log\nQuery History)]
    end

    subgraph OutputBlock ["Output Package"]
        SQL[SQL Executed]
        Assumptions[Assumptions Flagged]
        ResultTable[Result Table]
        Chart[Chart PNG or HTML]
        Summary[Plain-English Insight]
        CostLine[Scan Cost in USD]
    end

    LoadSchemas --> GlueCatalog
    LoadSchemas --> DbtArtifacts
    LoadSchemas --> User
    User --> GenerateSQL
    GenerateSQL --> ValidateSQL
    ValidateSQL -->|pass| Execute
    ValidateSQL -->|fail: reason sent back| GenerateSQL
    Execute --> Athena
    Athena --> S3Gold
    Execute --> TrackCost
    Execute --> ValidateResults
    ValidateResults --> RenderChart
    RenderChart --> Summarise
    Summarise --> OutputBlock
    Summarise --> AuditLog

    classDef box fill:#f0f4f8,stroke:#333,stroke-width:1px;
    class Startup,Input,AgentLoop,AWS,OutputBlock box;
```

---

## How the reasoning loop works

The agent starts each ECS task by loading all Gold schemas once and embedding them in the Claude system prompt. Claude knows every table and column before it sees the first question. This eliminates the multi-turn `list_tables` / `get_schema` tool-call round trips that text-to-SQL systems typically need.

### Startup: eager schema loading

`SchemaResolver.load_all_schemas()` runs once at startup. It reads all Gold tables from Glue Catalog (column names, data types, partition keys) and overlays dbt catalog.json from S3 (column descriptions, model documentation) for every table. The result — all 7 Gold schemas, roughly 2,500 tokens — is embedded directly in the system prompt.

If catalog.json isn't present yet (the pipeline hasn't run), it falls back to Glue-only schema and logs a warning. No crash, no partial startup.

### Step 1: Generate SQL in a single Claude call

Claude reads the question against the schema already in the system prompt and returns a SELECT query plus a list of assumptions (for example, "'transactions' interpreted as completed orders only"). No tool calls needed in the common case.

### Step 2: Validate

`SQLValidator` parses the query with sqlparse and enforces hard rules: SELECT only, Gold database only, no DDL keywords anywhere, LIMIT present. If validation fails, the error reason is sent back to Claude with a correction request. Up to 3 attempts before raising `SQLValidationError` to the user. The Athena WorkGroup `bytes_scanned_cutoff_per_query` setting in Terraform is the hard cost backstop.

### Step 3: Execute and track cost

`AthenaExecutor` starts the query, polls until complete, and reads the result CSV from the athena-results S3 bucket. `cost.py` converts `DataScannedInBytes` from the Athena execution metadata to USD. No pre-execution cost estimation needed — Gold tables are small pre-aggregations, and the WorkGroup hard stop handles any outliers.

### Step 4: Validate results

`ResultValidator` checks the DataFrame for obvious anomalies: negative values in revenue columns, unexpected nulls on key columns. Zero rows is a valid result for Gold tables — an aggregation with no matching data is a legitimate answer, not a bug. Flags are surfaced in the output, never block execution.

### Step 5: Chart and insight

`ChartGenerator` selects chart type from data shape: time-series data gets a line chart, 8 or fewer categories get a bar chart, more than 8 get a horizontal bar chart sorted by value. The chart is uploaded to S3 and returned as a presigned URL.

`InsightGenerator` makes a final Claude call with the original question, SQL, result sample, and assumptions, and returns a 2-3 sentence plain-English insight.

### Step 6: Audit

A structured JSON record is written to `s3://{bronze_bucket}/metadata/agent-audit/` containing the original question, SQL, assumptions, row count, bytes scanned, cost in USD, validation flags, and insight. The audit log is itself queryable via Athena.

---

## Guardrails

These are non-negotiable and enforced before any query reaches Athena.

| Guardrail | How it's enforced |
|---|---|
| SELECT only | sqlparse rejects anything that isn't a single SELECT statement |
| Gold DB only | Target database validated against `edp_{env}_gold` whitelist |
| LIMIT required | Injected if the model omits it, default 1000 rows |
| No DDL in any form | `DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER`, `TRUNCATE` rejected in statement or subquery |
| Partition filter required | Queries against large tables must include at least one partition key filter |
| Retry safety | Retry only on transient errors (throttling, timeout). Never on semantic failures (table not found, permission denied) |
| Cost hard stop | Athena WorkGroup `bytes_scanned_cutoff_per_query` in Terraform processing module |
| Read-only IAM | The agent's ECS task role has zero write permissions on Bronze, Silver, or Gold data |

---

## Schema auto-sync with MWAA

The MWAA DAG includes a final task `upload_dbt_artifacts` that runs after every successful `dbt test`. It copies `target/manifest.json` and `target/catalog.json` from the dbt workspace to `s3://{bronze_bucket}/metadata/dbt/`.

The agent reads this path at query time, never from a local cache. When a dbt model is renamed, a column description is updated, or a new Gold table is added, the agent sees the change automatically at the next query after the next pipeline run. Schema drift from dbt refactors is impossible because the agent never holds a stale copy.

---

## Deployment

The agent runs as an ECS (Elastic Container Service) Fargate task. It can be invoked two ways:

**CLI invocation** (for direct testing):
```bash
aws ecs run-task \
  --cluster edp-dev-cluster \
  --task-definition edp-dev-analytics-agent \
  --overrides '{"containerOverrides":[{"name":"agent","environment":[{"name":"QUESTION","value":"Show monthly revenue by country for Q1 2025"}]}]}' \
  --profile dev-admin
```

**HTTP invocation** (via FastAPI behind an ALB (Application Load Balancer)):
```bash
curl -X POST https://{alb-dns}/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Show monthly revenue by country for Q1 2025"}'
```

The response includes the SQL, assumptions, result table, chart presigned URL, insight, and scan cost.

### IAM role permissions

The ECS task role is defined in `terraform-platform-infra-live/modules/analytics-agent/main.tf` and scoped to exactly what the agent needs. Nothing more.

```
Athena:
  - athena:StartQueryExecution
  - athena:GetQueryExecution
  - athena:GetQueryResults
  - athena:StopQueryExecution

S3 (read):
  - s3:GetObject on {bronze_bucket}/metadata/dbt/*
  - s3:GetObject on {gold_bucket}/*
  - s3:GetObject, s3:PutObject on {athena_results_bucket}/*

S3 (write — agent outputs only):
  - s3:PutObject on {bronze_bucket}/metadata/agent-audit/*

Glue:
  - glue:GetTable
  - glue:GetDatabase
  - glue:GetPartitions
  on edp_{env}_gold database only

SSM:
  - ssm:GetParameter on /edp/{env}/anthropic_api_key
```

---

## Build phases

Each phase has a clear deliverable. No phase starts until the previous one passes `make lint`, `make typecheck`, and `make test`.

### Phase 1: Foundation — complete

Project skeleton with CI from the first commit. No business logic yet.

- `pyproject.toml`, `.python-version`, `requirements.txt`, `requirements-dev.txt`
- `Makefile` — setup, lint, typecheck, test, run targets
- `Dockerfile` (two-stage build, non-root user) + `docker-compose.yml`
- `.env.example`, `.gitignore`
- `agent/exceptions.py` — named exception hierarchy (`AgentError`, `SchemaResolutionError`, `SQLValidationError`, `CostLimitError`, `ExecutionError`, `ResultValidationError`)
- `agent/config.py` — frozen dataclasses driven by environment variables, fail fast at startup if any required variable is missing
- `agent/logging.py` — structured JSON logger used by every module from day one
- `.github/workflows/ci.yml` — ruff + mypy + pytest on every push
- `tests/conftest.py` — shared fixtures for mocked AWS clients and mocked Claude API responses

Deliverable: `make lint`, `make typecheck`, `make test` all pass. Docker image builds cleanly. CI is green.

### Phase 2: IAM and infra design — complete

Written before any AWS code so the executor is coded to the permission boundary, not retrofitted after.

All AWS infrastructure lives in `terraform-platform-infra-live/modules/analytics-agent/` — the same repo and state file as the rest of the platform. This is intentional: the agent's IAM role references bucket names, KMS key ARN, and Glue database name that are outputs of sibling modules. Keeping everything in one state file means no manual `tfvars` to maintain, and the teardown workflow covers the agent automatically.

Resources: ECR repository (scan on push, lifecycle policy keeps last 10 images), ECS cluster (FARGATE, Container Insights enabled), CloudWatch log group (30-day retention, KMS-encrypted), task execution role (ECR pull + CloudWatch write only), task IAM role (scoped exactly), security group (egress port 443 only), ECS task definition (512 CPU / 1024 MB, `lifecycle.ignore_changes` so CI updates the image without Terraform re-deploying).

Task IAM role grants: Gold S3 read-only, Athena results bucket read/write, Bronze `metadata/dbt/*` read, Bronze `metadata/agent-audit/*` write, Glue Gold catalog read-only, Athena query execution on the platform workgroup only, SSM API key read on `/edp/{env}/anthropic_api_key`, KMS decrypt on platform key only. No wildcard resources anywhere.

Deliverable: `terraform plan` in `terraform-platform-infra-live/environments/dev` produces the correct IAM role. All application AWS code is written inside this permission boundary.

### Phase 3: Schema resolver — in progress

The Gold layer has 7 small, pre-aggregated tables with 5-10 columns each. All schemas are loaded eagerly at startup and embedded in the system prompt — Claude knows every table and column before it sees the first question. This eliminates the `list_tables` / `get_schema` tool call round trips from the common case and is the single biggest latency saving in the design.

- `agent/schema.py` — `SchemaResolver` class:
  - `load_all_schemas()` — called once at startup. Reads `catalog.json` from `s3://{bronze_bucket}/metadata/dbt/` and fetches all Gold tables from `glue_client.get_tables()`. Merges physical schema (column names, types, partition keys) with business context (column descriptions, accepted values, model docs) for every table. Returns a single dict covering all 7 Gold tables (~2,500 tokens total). This dict is embedded directly in the system prompt so Claude starts every query with full schema awareness.
  - `get_schema(table_name)` — available as a tool for edge cases where Claude needs to re-examine one table during reasoning, but won't be called in normal operation.
  - Graceful fallback if `catalog.json` doesn't exist yet (pipeline hasn't run): falls back to Glue-only schema with a warning logged.
- `tests/test_schema.py` — parametrized tests with full mock fixtures for both Glue and S3 responses, including the fallback path.

Deliverable: `SchemaResolver.load_all_schemas()` returns the complete merged schema for all Gold tables in one call. Tested with and without `catalog.json` present.

### Phase 4: SQL validator

Guardrails are built before the SQL generator so no generated SQL can ever bypass them.

- `agent/validator.py` — `SQLValidator`:
  - Parses with sqlparse
  - Rejects anything that isn't a single SELECT statement
  - Rejects any DDL keyword anywhere in the statement or any subquery (`DROP`, `DELETE`, `INSERT`, `UPDATE`, `CREATE`, `ALTER`, `TRUNCATE`)
  - Rejects any database reference outside `edp_{env}_gold`
  - Injects `LIMIT 1000` if missing
  - Checks that at least one partition key filter is present for large tables
  - Returns validated SQL or raises `SQLValidationError` with a reason string Claude can act on
- `tests/test_validator.py` — parametrized, one test case per guardrail, both passing and failing inputs

Deliverable: `SQLValidator` enforces all guardrails. No SQL can reach Athena without passing through it.

### Phase 5: Prompts and Claude client

The agentic loop is a first-class module, not wired ad-hoc inside `main.py`.

- `agent/prompts.py` — all prompts in one place, reviewed and tuned independently of code:
  - System prompt: includes the full pre-loaded Gold schema dict from Phase 3, guardrail rules, and output format expectations. Because all schemas are embedded here, Claude can answer most questions in a single non-tool-call response.
  - SQL generation prompt: question + schema context → SELECT query + assumptions list
  - Insight prompt: question + SQL + result sample → 2-3 sentence plain-English insight
  - Tool definitions: `get_schema` (for edge cases where Claude needs to re-examine one table)
- `agent/claude_client.py` — `ClaudeClient`:
  - For the common case (question maps clearly to one Gold table): single Claude call, no tool use needed. Claude reads the schema from the system prompt and returns SQL + assumptions directly.
  - For edge cases (ambiguous question, needs to re-examine a specific table): handles `tool_use` content blocks, dispatches `get_schema`, sends `tool_result` back, repeats until text response.
  - Retries on transient errors (throttling, timeout) with exponential backoff.
  - Hard fails immediately on semantic errors (table not found, permission denied) with no retry.
- `tests/test_claude_client.py` — mocked Anthropic SDK responses covering single-turn (common case), tool-use fallback, and retry scenarios.

Deliverable: `ClaudeClient` handles both the single-call common path and the tool-use fallback path correctly. Retry behaviour tested.

### Phase 6: SQL generator with feedback loop

Gold queries are simple: `SELECT` from one pre-aggregated table with optional `WHERE` filters. A second review pass designed for complex JOINs adds latency and tokens with no benefit here. Single-pass generation with validation feedback is the right design.

- `agent/generator.py` — `SQLGenerator`:
  - Calls `ClaudeClient` with the question. Claude reads the schema from the system prompt and returns a SELECT query and a list of assumptions.
  - Runs the result through `SQLValidator`.
  - If validation fails, sends the error reason back to Claude and asks for a corrected query. Up to 3 attempts before raising `SQLValidationError` to the user.
  - No second review pass — Gold SQL is simple enough that sqlparse guardrail validation is sufficient.
  - Returns validated SQL and flagged assumptions.
- `tests/test_generator.py` — mocked `ClaudeClient`, tests the validation feedback loop, tests assumption extraction.

Deliverable: `SQLGenerator` handles validation failures gracefully and recovers automatically. Single Claude call in the common case.

### Phase 7: Athena executor and cost tracking

Gold tables are small pre-aggregated tables. The worst-case scan cost for any Gold query in a dev environment is a fraction of a cent — complex pre-execution cost estimation via Glue partition enumeration is unnecessary overhead. The Athena WorkGroup `bytes_scanned_cutoff_per_query` setting (configured in the Terraform processing module) is the hard cost backstop. Actual cost is tracked post-execution from the Athena result metadata and recorded in the audit log.

- `agent/executor.py` — `AthenaExecutor`:
  - `execute(sql)` — starts the Athena query, polls until complete, reads the result CSV from the S3 athena-results bucket.
  - Reads `Statistics.DataScannedInBytes` from the completed query execution and converts to USD (~$5 per TB scanned).
  - Returns a pandas DataFrame, actual bytes scanned, and actual cost in USD.
  - Retries on transient Athena errors (throttling, internal service error). Fails immediately on query errors (syntax, permission) with no retry.
- `agent/cost.py` — lightweight utility: one function that converts `DataScannedInBytes` to USD. No Glue calls, no S3 enumeration.
- `tests/test_executor.py` — mocked Athena start/poll/result cycle, failure handling, cost calculation tested.

Deliverable: Full Athena execution path works correctly. Actual cost tracked per query from execution metadata.

### Phase 8: Result validator, insight generator, and audit log

- `agent/result_validator.py` — `ResultValidator`: checks numeric values within plausible bounds (negative revenue is flagged), checks for unexpected nulls on key columns. Zero rows is a valid result for Gold tables — an aggregation with no matching data is a legitimate answer, not a bug. Returns a list of flags, never blocks execution, always surfaces flags in output.
- `agent/insight.py` — `InsightGenerator`: final Claude call that takes the original question, SQL, result DataFrame, and assumptions, and returns a 2-3 sentence plain-English insight. Uses the insight prompt from `prompts.py`. Structured output so malformed responses raise `AgentError`, not crash.
- `agent/audit.py` — `AuditLogger`: writes a structured JSON record to `s3://{bronze_bucket}/metadata/agent-audit/` after every query. Fields: question, SQL, assumptions, row count, bytes scanned, cost in USD, validation flags, insight, timestamp. The audit log is itself queryable via Athena.
- `tests/test_result_validator.py`, `tests/test_insight.py`

### Phase 9: CLI entry point and end-to-end integration

Wire all modules into the full loop.

- `agent/main.py` — orchestrates the complete reasoning chain: load schemas → generate SQL → validate → execute → validate results → generate insight → audit log → return output. Handles errors at each stage with clear user-facing messages. CLI entry point: `python -m agent.main "question"`.
- `tests/test_integration.py` — marked `@pytest.mark.integration`, runs against the real AWS dev environment, not mocks. Run manually before deploy, not in CI.

Deliverable: `python -m agent.main "Show total orders by country"` returns SQL, result table, flagged assumptions, and a 2-sentence insight against live Athena data in under 25 seconds. This is the core agent complete.

### Phase 10: Charts

- `agent/charts.py` — `ChartGenerator`:
  - Detects data shape from the DataFrame: time-series, category vs metric, or distribution
  - Time-series → line chart (matplotlib static PNG)
  - 8 or fewer categories → vertical bar chart
  - More than 8 categories → horizontal bar chart sorted by value
  - Uploads PNG to `s3://{bronze_bucket}/metadata/agent-charts/`, returns presigned URL
  - Plotly interactive HTML version for the HTTP endpoint

### Phase 11: FastAPI HTTP endpoint and session state

- FastAPI route added to `agent/main.py` — POST `/query` accepts `{"question": "...", "session_id": "..."}`, returns full JSON response: SQL, assumptions, result table, presigned chart URL, insight, scan cost
- Session state keyed by `session_id` maintains conversation history for multi-turn follow-ups ("now break it down by region")

### Phase 12: Deploy pipeline and ECS infra

- `.github/workflows/deploy.yml` — CI passes → Docker build → push to ECR (Elastic Container Registry) → update ECS task definition → smoke test against dev Athena
- `infra/` expanded with ALB (Application Load Balancer), ECR repository, and ECS service
- Demo script covering 6 showcase questions across all chart types

---

## Performance and cost per query

The Gold layer is pre-aggregated. Each table directly answers a specific business question with 5-10 columns and tens to low hundreds of rows. All 7 Gold schemas (~2,500 tokens total) are loaded at startup and embedded in the system prompt, so Claude knows every table before it sees the first question. This eliminates the multi-turn schema resolution loop and is the single biggest design decision affecting latency and cost.

### Response time per question

| Step | Time |
|---|---|
| Claude call 1: schema already in prompt, generate SQL + assumptions | 6-10s |
| SQL validation (local, sqlparse) | <0.1s |
| Athena execution on small Gold table | <2s |
| Result validation (local, pandas) | <0.1s |
| Claude call 2: insight generation | 3-5s |
| Chart generation + S3 upload | 1-2s |
| Audit log write | 0.2s |
| **Typical total** | **12-20 seconds** |

### Token usage per question

| Component | Input tokens | Output tokens |
|---|---|---|
| System prompt with all Gold schemas | ~2,500 | — |
| User question | ~30 | — |
| SQL + assumptions | — | ~200 |
| Insight prompt + question + result sample | ~700 | ~150 |
| **Total per question** | **~3,230** | **~350** |

### Cost per question

Claude-sonnet-4-6 pricing: $3.00 per million input tokens, $15.00 per million output tokens.

| Component | Cost |
|---|---|
| Claude API (~3,230 input + ~350 output tokens) | ~$0.015 |
| Athena scan (Gold table, <5 MB) | <$0.001 |
| S3 operations (audit log, chart upload) | <$0.001 |
| **Total per question** | **~$0.016** |

### 50-question demo session

| Component | Per session cost |
|---|---|
| ECS Fargate (0.5 vCPU, 1 GB, 3 hours) | ~$0.08 |
| Claude API (50 questions × ~$0.016) | ~$0.80 |
| Athena (50 queries, <5 MB each) | ~$0.001 |
| S3 (audit logs, chart PNGs) | ~$0.01 |
| ALB (3 hours, if HTTP endpoint used) | ~$0.05 |
| **Total per session** | **~$0.94** |

Claude API cost dominates. Athena cost on Gold tables is negligible. The pre-aggregated Gold layer cuts both response time and Claude token usage roughly in half compared to querying Silver directly.

---

## Example interaction

**Question:** "Show me monthly transaction volume for Berlin over the last 12 months"

**Agent output:**

```
Interpretation (please confirm before I proceed):
  Table:   gold.monthly_revenue_trend (via fct_orders)
  Filter:  city = 'Berlin', order_date between 2024-04-01 and 2025-03-31
  Metric:  COUNT(DISTINCT order_id) grouped by year_month
  Note:    'transactions' interpreted as placed orders with status = 'completed'

SQL executed:
  SELECT
    date_trunc('month', order_date) AS month,
    COUNT(DISTINCT order_id) AS transaction_volume
  FROM edp_dev_gold.fct_orders
  WHERE city = 'Berlin'
    AND order_date >= DATE '2024-04-01'
    AND order_date < DATE '2025-04-01'
    AND status = 'completed'
  GROUP BY 1
  ORDER BY 1
  LIMIT 1000

Result: 12 rows returned
Bytes scanned: 4.3 MB  |  Cost: $0.000022

Insight:
Berlin completed orders peaked in November 2024 at 1,847 transactions,
driven by seasonal demand. Volume has been broadly stable through Q1 2025
at around 1,400 to 1,500 monthly transactions, roughly 12% above the same
period in 2024.

Chart: [presigned S3 URL — time series line chart]
```

---

## Tech stack

| Tool | What it does |
|---|---|
| Python 3.11.8 | Agent runtime |
| Claude API (claude-sonnet-4-6) | Question interpretation, SQL generation, insight summarisation |
| boto3 | AWS SDK: Athena, Glue Catalog, S3, SSM |
| sqlparse | SQL parsing and validation |
| FastAPI | HTTP endpoint |
| matplotlib | Static chart PNG generation |
| Plotly | Interactive chart HTML generation |
| ECS Fargate | Serverless container runtime |
| Amazon Athena | Executes generated SQL against Gold S3 data |
| AWS Glue Data Catalog | Live physical schema: column names, types, partition keys |
| dbt catalog.json | Business schema context: descriptions, accepted values, documentation |
| pytest | Unit and integration testing |
| ruff | Python linting |
| mypy | Static type checking |
| Docker | Local development and CI builds |

---

## Repository structure

```
platform-analytics-agent/
├── agent/                      ← Python agent source code
│   ├── main.py                 ← CLI entry point and FastAPI app
│   ├── config.py               ← frozen dataclasses, env var validation, fail fast on missing vars
│   ├── exceptions.py           ← named exception hierarchy
│   ├── logging.py              ← structured JSON logger used by every module
│   ├── prompts.py              ← all Claude prompts in one place: system prompt (with schemas), insight
│   ├── claude_client.py        ← Claude API client: single-call common path, tool-use fallback, retry
│   ├── schema.py               ← schema resolver: load_all_schemas() at startup, Glue + dbt catalog.json
│   ├── validator.py            ← SQL validator: sqlparse guardrail rules, SELECT-only, Gold DB only
│   ├── generator.py            ← SQL generator: single-pass, validation feedback loop (3 attempts)
│   ├── cost.py                 ← lightweight utility: converts DataScannedInBytes to USD
│   ├── executor.py             ← Athena SDK: execute, poll, read results from S3, record actual cost
│   ├── result_validator.py     ← result sanity checks: numeric bounds, null rates (zero rows is valid)
│   ├── insight.py              ← insight generator: final Claude call, structured output
│   ├── charts.py               ← matplotlib PNG and Plotly HTML chart generation
│   └── audit.py                ← structured JSON audit log writer to S3
│   (no infra/ directory — all AWS infrastructure lives in terraform-platform-infra-live)
├── tests/                      ← pytest unit and integration tests
│   ├── conftest.py             ← shared fixtures: mocked boto3 clients, mocked Claude responses
│   ├── test_config.py
│   ├── test_exceptions.py
│   ├── test_schema.py
│   ├── test_validator.py
│   ├── test_claude_client.py
│   ├── test_generator.py
│   ├── test_executor.py        ← includes cost conversion tests
│   ├── test_result_validator.py
│   ├── test_insight.py
│   └── test_integration.py     ← marked @pytest.mark.integration, runs against real AWS dev
├── .python-version             ← 3.11.8 (pyenv)
├── pyproject.toml              ← ruff, mypy, pytest config
├── requirements.txt            ← runtime dependencies
├── requirements-dev.txt        ← dev tools: ruff, mypy, pytest
├── Dockerfile                  ← two-stage build, non-root user
├── docker-compose.yml          ← local dev
├── Makefile                    ← setup, lint, typecheck, test, run
└── README.md                   ← this file
```

---

## Status

**In development.** Phases 1 and 2 are complete. Phase 3 (schema resolver) is in progress.

| Phase | Status |
|---|---|
| 1: Foundation (skeleton, CI, Docker, exceptions, config, logging) | Complete |
| 2: IAM and infra (ECR, ECS cluster, task role, task definition) | Complete |
| 3: Schema resolver (Glue + dbt catalog.json → system prompt) | In progress |
| 4: SQL validator | Not started |
| 5: Prompts and Claude client | Not started |
| 6: SQL generator with feedback loop | Not started |
| 7: Athena executor and cost tracking | Not started |
| 8: Result validator, insight generator, audit log | Not started |
| 9: CLI entry point and end-to-end integration | Not started |
| 10: Charts | Not started |
| 11: FastAPI HTTP endpoint and session state | Not started |
| 12: Deploy pipeline and full ECS infra | Not started |

This is the last component of the platform. Everything else is complete and validated end-to-end in AWS.
