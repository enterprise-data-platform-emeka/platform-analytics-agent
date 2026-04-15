# platform-analytics-agent

This repository is part of the [Enterprise Data Platform](https://github.com/enterprise-data-platform-emeka/platform-docs). For the full project overview, architecture diagram, and build order, start there.

**Previous:** [platform-orchestration-mwaa-airflow](https://github.com/enterprise-data-platform-emeka/platform-orchestration-mwaa-airflow): the Airflow DAG on MWAA orchestrates the pipeline that produces the Gold tables this agent queries.

---

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
        Streamlit[Streamlit UI\nBrowser interface]
        User --> Streamlit
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
    LoadSchemas -->|pre-loads into system prompt| GenerateSQL
    Streamlit -->|POST /ask| GenerateSQL
    GenerateSQL --> ValidateSQL
    ValidateSQL -->|pass| Execute
    ValidateSQL -->|fail: reason sent back| GenerateSQL
    Execute --> Athena
    Athena -->|reads| S3Gold
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

## Stakeholder interface

The FastAPI backend is the analytical engine. Non-technical stakeholders don't interact with it directly. The user-facing layer is a Streamlit browser app that wraps the backend.

A stakeholder opens a URL in their browser, types a plain-English question in a text box, and clicks submit. Streamlit POSTs the question to the FastAPI `/ask` endpoint. When the response arrives, Streamlit displays everything inline: the plain-English insight, the chart as an inline image (fetched from the presigned S3 URL (Uniform Resource Locator) the agent returns), the SQL in an expandable section, the list of assumptions the agent made, and the scan cost in USD.

No command-line access needed. No SQL knowledge needed. No understanding of Athena, Glue, or partition structures required. That's the whole point.

The Streamlit app runs in the same ECS (Elastic Container Service) Fargate container as the FastAPI backend. A startup script (`entrypoint.sh`) launches both together when the container starts: FastAPI on port 8080, Streamlit on port 8501. The AWS load balancer (ALB) has a separate listener for each port, so stakeholders open the Streamlit URL and FastAPI stays reachable for direct API access. For local development, the same startup script runs both with a single `docker-compose up`.

---

### How the browser interface works

You don't need any web development experience to understand this. Here's what actually happens, step by step.

**What Streamlit is**

Streamlit is a Python library. You write Python code and Streamlit turns it into a web page with a text box, buttons, and charts. There's no HTML, CSS, or JavaScript involved. The entire UI is Python, which fits naturally into a project where everything else is already Python.

**What "running on a server" means**

When you type a web address into Chrome or Safari, your browser connects to a program that is listening for incoming connections. That program is called a server. In this project, Streamlit is that server. When a stakeholder opens `http://alb-url:8501`, their browser connects to the Streamlit program running inside the ECS container in AWS. Streamlit sends the web page back to their browser. This is no different from visiting any website: there's always a server somewhere that sends you the page.

**Why the code calls localhost**

Both Streamlit and FastAPI run inside the same ECS container. Think of a container as a small private computer with its own isolated network. When the Streamlit code calls `localhost:8080`, it means "call the program listening on port 8080 inside this container" — which is FastAPI. The stakeholder's browser never sees or uses localhost. They only ever type the ALB's DNS address. Localhost is purely internal, invisible to the outside world.

```mermaid
sequenceDiagram
    participant SB as Stakeholder's Browser
    participant ALB as AWS Load Balancer
    participant ST as Streamlit (port 8501)
    participant API as FastAPI (port 8080)
    participant AWS as Athena + Claude API

    Note over ST,API: Both run inside the same ECS container

    SB->>ALB: Opens http://alb-url:8501
    ALB->>ST: Routes request to Streamlit
    ST-->>SB: Sends the web page (text box and Submit button)

    Note over SB: Stakeholder types a question and clicks Submit

    SB->>ST: Submits the question
    ST->>API: POST localhost:8080/ask (same container, no network hop)
    API->>AWS: Runs Athena query, calls Claude API
    AWS-->>API: Query results and insight
    API-->>ST: JSON with insight, chart HTML, cost
    ST-->>SB: Renders insight, interactive chart, and cost in the browser
```

The stakeholder never runs a command. They open a URL, type a question, and read the answer. Everything else happens inside AWS.

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

`ChartGenerator` selects chart type from data shape: time-series data gets a line chart, 8 or fewer categories get a bar chart, more than 8 get a horizontal bar chart sorted by value. The chart is uploaded to S3 and returned as a presigned URL (Uniform Resource Locator).

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

## What you can ask

The agent queries the Gold layer. There are 7 mart tables, each designed to answer a specific category of business question. The questions below are grounded in the exact columns those tables expose.

Understanding the boundaries matters as much as knowing what works. Some questions sound reasonable but can't be answered with the current data, either because the mart doesn't have the right dimension, or because that field was never captured anywhere in the pipeline. Both sets are listed below.

---

### Revenue and finance

**`monthly_revenue_trend`** — one row per year-month. Columns: `order_year`, `order_month`, `total_orders`, `unique_customers`, `total_revenue`, `cancelled_orders`.

- What were total sales last month?
- Show me revenue by month for this year.
- How has total order volume changed over the past 12 months?
- Which month had the highest number of cancellations?
- How many unique customers placed an order in each month this year?
- Is revenue trending up or down compared to the same month last year?

**`revenue_by_country`** — one row per country, all-time totals for completed orders. Columns: `country`, `total_orders`, `total_customers`, `total_revenue`, `avg_order_value`.

- Which country generates the most revenue?
- What is the average order value for customers in Germany?
- How many distinct customers have placed completed orders in France?
- Rank all countries by total revenue.
- Which country has the highest average order value?

**`payment_method_performance`** — one row per payment method. Columns: `payment_method`, `total_transactions`, `successful`, `failed`, `refunded`, `success_rate_pct`, `total_processed`, `revenue_captured`.

- Which payment method has the highest failure rate?
- How much revenue was lost to refunds across all payment methods?
- What percentage of bank transfer payments complete successfully?
- Which payment method processes the most total volume?
- How many transactions were refunded via credit card?

---

### Products

**`top_selling_products`** — one row per product. Columns: `product_id`, `product_name`, `category`, `brand`, `total_orders`, `total_units_sold`, `total_revenue`, `avg_revenue_per_unit`, `revenue_rank`.

- What are the top 10 best-selling products by total revenue?
- Which product has sold the most units?
- What is the average revenue per unit for the top 5 products?
- Which brand appears most often in the top 20 products by revenue rank?
- Which product generates the most revenue per unit sold?

**`product_category_performance`** — one row per category-brand combination. Columns: `category`, `brand`, `total_orders`, `products_in_category`, `total_units_sold`, `total_revenue`, `avg_revenue_per_unit`.

- Which product category generates the most revenue?
- Which brand has the highest revenue across all categories?
- How many distinct products has each category sold?
- Which category sells the most units?
- Compare average revenue per unit across Electronics, Clothing, and Sports.

---

### Logistics and delivery

**`carrier_delivery_performance`** — one row per carrier. Columns: `carrier`, `total_shipments`, `delivered`, `failed`, `delivery_success_rate_pct`, `avg_delivery_days`, `fastest_delivery_days`, `slowest_delivery_days`.

- Which carrier has the highest delivery success rate?
- What is the average delivery time for DHL?
- Which carrier has the most failed shipments?
- How do FedEx and UPS compare on average delivery days?
- Which carrier achieves the fastest single delivery on record?
- What is the spread between fastest and slowest delivery times for each carrier?

---

### Customers

**`customer_segments`** — one row per customer. Columns: `customer_id`, `first_name`, `last_name`, `email`, `country`, `signup_date`, `total_orders`, `lifetime_value`, `first_order_date`, `last_order_date`, `segment` (VIP / Regular / Low Value / Never Ordered), `order_frequency_band` (Loyal / Occasional / One-Time / No Orders).

- How many VIP customers do we have?
- Which country has the most Loyal customers?
- Who are the top 10 customers by lifetime value?
- How many customers have never placed an order?
- What share of customers are one-time buyers?
- Which customers signed up in the last 90 days and are already VIP?
- How is lifetime value distributed across segments in the UK?

---

### Multilingual questions

The agent responds in the same language as the question. Any language Claude supports works. Here are three examples grounded in the actual Gold tables.

**German**

- *Welche Zahlungsmethode hat die höchste Ausfallrate, und wie viel Umsatz ging dadurch verloren?*
  (Which payment method has the highest failure rate, and how much revenue was lost as a result?)

- *Wie hat sich der monatliche Gesamtumsatz in diesem Jahr im Vergleich zum Vorjahreszeitraum entwickelt?*
  (How has total monthly revenue developed this year compared to the same period last year?)

- *Welche fünf Kunden haben den höchsten Lifetime Value, und aus welchem Land kommen sie?*
  (Which five customers have the highest lifetime value, and which country are they from?)

**Chinese (Simplified)**

- *哪个运输承运商的平均配送天数最少，成功交付率最高？*
  (Which shipping carrier has the lowest average delivery days and the highest delivery success rate?)

- *按国家划分，哪个市场的总收入最高，平均订单价值是多少？*
  (By country, which market has the highest total revenue, and what is the average order value?)

- *在所有产品类别中，哪个品牌的单位平均收入最高？*
  (Across all product categories, which brand has the highest average revenue per unit?)

**Italian**

- *Quali sono i 10 prodotti più venduti per fatturato totale e quante unità ha venduto ciascuno?*
  (What are the top 10 products by total revenue, and how many units has each sold?)

- *Quanti clienti VIP abbiamo e qual è il loro valore medio nel corso della vita?*
  (How many VIP customers do we have, and what is their average lifetime value?)

- *Qual è il tasso di successo delle transazioni per ogni metodo di pagamento?*
  (What is the transaction success rate for each payment method?)

---

### Questions the agent cannot answer

These questions sound reasonable but cannot be answered with the current Gold layer. The reason for each is specific: either the mart lacks a dimension, or the field was never captured anywhere in the pipeline.

**No time dimension in `revenue_by_country`**

> "What was Germany's revenue last month?"

`revenue_by_country` is a lifetime aggregation with no `order_year` or `order_month` column. `monthly_revenue_trend` has time but no country. There's no mart that combines both. The agent will say it can't answer this precisely and explain why.

**No country dimension in `monthly_revenue_trend`**

> "Show me monthly revenue broken down by country."

Same problem in reverse. The two finance marts cover different cuts of the same underlying data but there's no mart that crosses both dimensions. Adding one would require a new dbt model.

**Stock levels not in Gold**

> "Which products are running low on inventory?"

`stock_qty` exists in the Silver `products` table and flows through `stg_products`, but no Gold mart exposes it. It was excluded because it's a point-in-time operational field, not a business aggregation. The agent has no visibility into it.

**No cost-of-goods data**

> "What is the gross margin on Electronics?"

Revenue is captured throughout the pipeline (`line_total`, `payment_amount`), but cost of goods sold is not. There is no `unit_cost`, `cogs`, or `margin` column anywhere from the OLTP source through to Gold. Margin questions of any kind cannot be answered.

**No shipping cost data**

> "Which carrier is the most cost-effective?"

`carrier_delivery_performance` tracks delivery days and success rates but not the fee charged per shipment. Shipping cost is not in the source OLTP schema and therefore not anywhere in the pipeline.

**No city or region geography**

> "Which cities in France drive the most revenue?"

Customer geography is captured at country level only. The CDC simulator generates a `country` field but no city, region, state, or postal code. These fields don't exist in Silver or Gold.

**No real-time data**

> "What orders came in the last hour?"

Gold is updated by the daily batch pipeline. The latest data reflects the most recent successful `edp_pipeline` run. The agent can't see anything more recent than that.

**No marketing or acquisition data**

> "Which marketing channel brings the most VIP customers?"

The OLTP system records what customers ordered, not how they were acquired. There is no campaign, referral, UTM parameter, or acquisition source anywhere in the data model.

**No cart abandonment or browse data**

> "Which products do customers view but not buy?"

The platform captures CDC events from a PostgreSQL OLTP database. Only database writes are tracked, which means only orders that were created. Browse events, add-to-cart actions, and session data don't exist.

**No refund reason or return details**

> "What is the most common reason customers return Electronics?"

`payment_method_performance` tracks a refund count per payment method, but there is no refund reason, return category, or customer comment field anywhere. The OLTP schema has a `status` field on payments but no reason code.

**No product co-purchase patterns**

> "Customers who buy X also buy Y — what are the top product pairs?"

There's no basket analysis mart. `top_selling_products` and `product_category_performance` aggregate each product independently. Building this would require a self-join on order items at the intermediate layer, which hasn't been modelled.

---

## End-to-end testing

This section explains exactly where you type your question and what to do step by step. There are two ways to ask the agent a question.

---

### Quick-start checklist (do this when you come back tomorrow)

Run these in order. Each one takes about a minute. If any step fails, the numbered troubleshooting items below will tell you exactly what to do.

```
[ ] 1. Buy Anthropic API credits at console.anthropic.com > Plans & Billing
[ ] 2. Apply infra:   cd terraform-platform-infra-live && make apply dev
[ ] 3. Trigger the Airflow DAG (edp_pipeline) in the MWAA console and wait for it to finish
[ ] 4. Test locally:  cd platform-analytics-agent && source .venv/bin/activate
                      export $(grep -v '^#' .env | xargs)
                      python -m agent.main "Which country has the highest total revenue?"
[ ] 5. If local works, test ECS:  see Track 2 below
[ ] 6. Destroy when done:  cd terraform-platform-infra-live && make destroy dev
```

Step 4 is where you type your question. The agent runs on your Mac, talks to AWS, and prints the answer directly in your terminal. If you only have a few minutes, steps 1-4 are all you need.

---

**Track 1 (recommended, start here):** You type your question in your Mac terminal. The agent code runs on your Mac but connects to the real AWS dev environment (real Athena, real Glue, real S3). No ECS needed. This is the fastest way to iterate and confirms all AWS integrations are working.

**Track 2:** The agent is deployed to ECS Fargate. You shell into the running container using the AWS CLI and make HTTP requests to the agent's FastAPI server from inside the container. This confirms the deployed Docker image and ECS service are healthy.

Start with Track 1. Only move to Track 2 once Track 1 is producing correct answers.

---

### Prerequisites (do these once before the first test)

**1. Store your Anthropic API key in SSM Parameter Store.**

The agent fetches its API key from AWS Systems Manager (SSM) at startup. It never reads it from a file or environment variable directly — this way the key never appears in ECS task logs or Terraform state.

Go to the AWS console, make sure you're in `eu-central-1`, and navigate to Systems Manager > Parameter Store. Create a new parameter with these exact values:

| Field | Value |
|---|---|
| Name | `/edp/dev/anthropic_api_key` |
| Type | `SecureString` |
| Value | Your Anthropic API key (starts with `sk-ant-`) |
| KMS key | Use the default `aws/ssm` key |

You only need to do this once. The parameter survives `terraform destroy` because it's not managed by Terraform.

Alternatively, do it from the terminal:

```bash
aws ssm put-parameter \
  --name "/edp/dev/anthropic_api_key" \
  --type "SecureString" \
  --value "sk-ant-YOUR-KEY-HERE" \
  --profile dev-admin \
  --region eu-central-1
```

To verify it was stored:

```bash
aws ssm get-parameter \
  --name "/edp/dev/anthropic_api_key" \
  --with-decryption \
  --profile dev-admin \
  --region eu-central-1
```

**2. Confirm the MWAA pipeline has run and Gold data exists.**

The agent queries the Gold Athena tables. If the pipeline hasn't run yet, every query will return zero rows (or a table-not-found error if the Glue Catalog is empty). Log into the Airflow UI for your MWAA environment (`edp-dev-mwaa`) and confirm `edp_pipeline` has at least one successful DAG run. If it hasn't, trigger it manually and wait for it to complete (about 6-8 minutes).

You can also confirm Gold data exists by running a quick Athena query in the AWS console:

```sql
SELECT COUNT(*) FROM "edp_dev_gold"."fct_orders" LIMIT 1;
```

If this returns a count greater than zero, the Gold layer is ready.

**3. Confirm the deploy workflow completed.**

When you pushed the latest code changes to `main`, GitHub Actions ran the CI workflow first (lint, type check, unit tests, Docker build). Once CI passed, the Deploy workflow triggered automatically and built the Docker image, pushed it to ECR (Elastic Container Registry), and updated the ECS task definition.

Go to the GitHub repository for `platform-analytics-agent`, click Actions, and confirm both the CI and Deploy workflows have a green tick for your latest push. If Deploy is still running, wait for it to finish before testing Track 2.

---

### Track 1: Local functional test (start here)

This runs the agent Python code directly on your Mac against the real AWS dev environment. It uses your local `dev-admin` AWS profile for credentials, connects to real Athena, real Glue Catalog, and real SSM. The output is identical to what you'd see in ECS — the only difference is that the compute runs on your Mac instead of Fargate.

**Step 1: Create your `.env` file.**

Copy `.env.example` to `.env`:

```bash
cd platform-analytics-agent
cp .env.example .env
```

Open `.env` and fill in the values. The only things you need to change are the bucket names and your AWS account ID. You can find your account ID by running:

```bash
aws sts get-caller-identity --profile dev-admin --query Account --output text
```

Update `.env` with your account ID substituted in:

```
AWS_REGION=eu-central-1
AWS_PROFILE=dev-admin
ENVIRONMENT=dev
BRONZE_BUCKET=edp-dev-YOUR_ACCOUNT_ID-bronze
GOLD_BUCKET=edp-dev-YOUR_ACCOUNT_ID-gold
ATHENA_RESULTS_BUCKET=edp-dev-YOUR_ACCOUNT_ID-athena-results
ATHENA_WORKGROUP=edp-dev-workgroup
GLUE_GOLD_DATABASE=edp_dev_gold
SSM_API_KEY_PARAM=/edp/dev/anthropic_api_key
COST_THRESHOLD_USD=0.10
MAX_ROWS=1000
```

**Step 2: Activate the virtual environment.**

```bash
source .venv/bin/activate
```

If you haven't run `make setup` yet:

```bash
make setup
source .venv/bin/activate
```

**Step 3: Load the `.env` file.**

The agent reads these values from environment variables, not from the `.env` file directly. Load them into your shell session:

```bash
export $(grep -v '^#' .env | xargs)
```

**Step 4: Ask your first question.**

```bash
python -m agent.main "Which country has the highest total revenue?"
```

The agent takes 12-20 seconds to respond. On the first run there's an additional few seconds for schema loading (Glue Catalog + dbt catalog.json from S3). What you'll see printed:

- The SQL it generated
- Every assumption it made ("'revenue' interpreted as the `total_price` column on completed orders")
- The result table
- Bytes scanned and cost in USD
- A 2-3 sentence plain-English insight
- A presigned S3 URL (Uniform Resource Locator) for the chart PNG

**Step 5: Try these test questions.**

Run each one with `python -m agent.main "question"`. They're ordered to cover different Gold tables, different chart types, and different kinds of reasoning.

```bash
# Bar chart — top categories, single aggregation
python -m agent.main "Show me total revenue by country"

# Line chart — time-series, tests date reasoning
python -m agent.main "What does monthly order volume look like over the last 12 months?"

# Filtering + aggregation — tests WHERE clause generation
python -m agent.main "Which product categories are most popular in Germany?"

# Multi-metric — tests selecting the right columns
python -m agent.main "Compare average order value across countries"

# Count + group by — tests COUNT vs SUM disambiguation
python -m agent.main "How many unique customers placed orders in each country?"

# Trend question — tests the agent's ability to reason about time
python -m agent.main "Is revenue growing or declining? Show me the trend."
```

**What to look for in each response:**

- The SQL should have a `WHERE` clause with a partition filter (e.g., `dt >= ...`) — this confirms partition-aware query generation is working
- The assumptions list should explain any interpretation decisions Claude made
- The insight should be specific to the actual numbers in the result, not generic
- The presigned URL should be a real S3 URL — open it in a browser to see the chart
- Bytes scanned should be small (under 10 MB for Gold tables) — confirms the partition filters are working
- Cost should be under $0.001 per query — confirms the Gold layer's efficiency

**Step 6: Test multi-turn follow-up.**

For multi-turn follow-up (where the agent remembers the previous question), you need the HTTP endpoint. That's covered in Track 2. The CLI mode (`python -m agent.main`) is single-turn only — each invocation is independent.

---

### Track 2: Test the deployed ECS service

Once Track 1 passes, this confirms the Docker image running on ECS Fargate is working. The ALB (Application Load Balancer) in front of the service is internal-only — it can't be reached from your browser directly. Instead, you use `aws ecs execute-command` to open a shell inside the running container and make HTTP requests from there.

**Step 1: Confirm the ECS service has a running task.**

```bash
aws ecs describe-services \
  --cluster edp-dev-analytics-agent \
  --services edp-dev-analytics-agent \
  --profile dev-admin \
  --region eu-central-1 \
  --query "services[0].{running:runningCount,pending:pendingCount,taskDef:taskDefinition}" \
  --output json
```

You want `running: 1`. If it shows `running: 0`, wait a minute and run it again. If it stays at zero, check the CloudWatch logs (Step 3).

**Step 2: Get the running task ID.**

```bash
aws ecs list-tasks \
  --cluster edp-dev-analytics-agent \
  --desired-status RUNNING \
  --profile dev-admin \
  --region eu-central-1 \
  --query "taskArns[0]" \
  --output text
```

This returns a long ARN. The task ID is the last segment after the final `/`. For example, from:

```
arn:aws:ecs:eu-central-1:123456789012:task/edp-dev-analytics-agent/3e4ea78b0be54d61
```

The task ID is `3e4ea78b0be54d61`. You'll use this in the next steps.

**Step 3: Check the health endpoint.**

Replace `TASK_ID` with the value from Step 2.

```bash
aws ecs execute-command \
  --cluster edp-dev-analytics-agent \
  --task TASK_ID \
  --container agent \
  --interactive \
  --command "python -c \"import urllib.request; print(urllib.request.urlopen('http://localhost:8080/health').read().decode())\"" \
  --profile dev-admin \
  --region eu-central-1
```

Expected output: `{"status":"ok"}`

If you get `SessionManagerPlugin is not found`, install it first:

```bash
brew install --cask session-manager-plugin
```

**Step 4: Ask your first question to the ECS-deployed agent.**

This is where you type your question. Replace `TASK_ID` with your task ID and replace the question text with anything you want to ask.

```bash
aws ecs execute-command \
  --cluster edp-dev-analytics-agent \
  --task TASK_ID \
  --container agent \
  --interactive \
  --command "python -c \"
import urllib.request, json
body = json.dumps({'question': 'Which country has the highest total revenue?'}).encode()
req = urllib.request.Request(
    'http://localhost:8080/ask',
    data=body,
    headers={'Content-Type': 'application/json'},
    method='POST'
)
resp = urllib.request.urlopen(req, timeout=120)
data = json.loads(resp.read().decode())
print()
print('INSIGHT:', data['insight'])
print()
print('SQL EXECUTED:')
for a in data.get('assumptions', []):
    print(' -', a)
print()
print('BYTES SCANNED:', data['bytes_scanned'])
print('COST USD:', data['cost_usd'])
print('CHART TYPE:', data['chart_type'])
print('CHART URL:', data.get('presigned_url'))
print('SESSION ID:', data['session_id'])
\"" \
  --profile dev-admin \
  --region eu-central-1
```

The command takes 15-30 seconds. The agent is loading schemas from Glue, generating SQL with Claude, running the Athena query, and producing the insight.

**What you'll see in the output:**

- `INSIGHT` — a 2-3 sentence plain-English answer to your question based on the actual data
- The assumptions list — what the agent interpreted (for example, "'revenue' means total price on completed orders only")
- `BYTES SCANNED` — how much Athena data was read (should be small for Gold tables, under 10 MB)
- `COST USD` — the Athena query cost (should be under $0.001)
- `CHART URL` — an S3 presigned URL. Copy it and open it in your browser to see the chart image.
- `SESSION ID` — copy this if you want to ask a follow-up question

**Step 5: Ask a follow-up question (multi-turn).**

Copy the `SESSION ID` from Step 4 and paste it into this command as `YOUR_SESSION_ID`. The agent will remember the previous question and answer.

```bash
aws ecs execute-command \
  --cluster edp-dev-analytics-agent \
  --task TASK_ID \
  --container agent \
  --interactive \
  --command "python -c \"
import urllib.request, json
body = json.dumps({
    'question': 'Now break that down by city',
    'session_id': 'YOUR_SESSION_ID'
}).encode()
req = urllib.request.Request(
    'http://localhost:8080/ask',
    data=body,
    headers={'Content-Type': 'application/json'},
    method='POST'
)
resp = urllib.request.urlopen(req, timeout=120)
data = json.loads(resp.read().decode())
print('INSIGHT:', data['insight'])
print('SESSION ID:', data['session_id'])
\"" \
  --profile dev-admin \
  --region eu-central-1
```

**Step 6: Read the CloudWatch logs if something goes wrong.**

All errors and startup messages are written to CloudWatch. This is the first place to look when a request fails.

```bash
aws logs tail /ecs/edp-dev-analytics-agent \
  --follow \
  --profile dev-admin \
  --region eu-central-1
```

Press Ctrl-C to stop tailing. The most common startup errors are documented in the troubleshooting section below.

---

### Suggested test questions for a full demo

These six questions cover every Gold table, every chart type, and several multi-turn follow-up patterns. Run them in sequence during a demo session.

| # | Question | What it tests |
|---|---|---|
| 1 | "Which country has the highest total revenue?" | Bar chart, top-N aggregation |
| 2 | "Show me monthly order volume as a trend over the last year" | Line chart, time-series, date reasoning |
| 3 | "What are the top 5 product categories by revenue in Germany?" | Filtered bar chart, WHERE clause with partition |
| 4 | "Compare average order value across all countries" | Horizontal bar chart (many categories) |
| 5 | "How many unique customers have placed orders in each country?" | COUNT DISTINCT, tests SUM vs COUNT disambiguation |
| 6 | "Is there a seasonal pattern in order volume?" | Line chart, tests pattern-recognition insight generation |

For multi-turn follow-up, after question 2 ask: "Which month had the lowest volume and why do you think that is?" — the agent will answer referencing the same SQL execution without re-running the query.

---

### What to do if something goes wrong

**"Missing required environment variables"** — You forgot to `export $(grep -v '^#' .env | xargs)` before running the command, or a variable name is misspelled in your `.env` file.

**"SchemaResolutionError: Glue Catalog unreachable"** — The infra isn't up, or your `dev-admin` profile doesn't have permission to call `glue:GetTables`. Confirm `terraform apply` completed successfully and the `edp_dev_gold` Glue database exists in the AWS console.

**"No tables found in Gold database"** — The MWAA pipeline hasn't run yet. Trigger `edp_pipeline` in the Airflow UI and wait for it to complete.

**"ParameterNotFound: /edp/dev/anthropic_api_key"** — You skipped the SSM prerequisite step. Run the `aws ssm put-parameter` command from the Prerequisites section.

**"AccessDenied on SSM GetParameter"** — The ECS task role doesn't have permission to read this SSM path. Confirm `terraform apply` ran successfully and the `analytics-agent` module is included.

**"Athena query failed: TABLE_NOT_FOUND"** — Either the Gold Glue Catalog tables don't exist (run the MWAA pipeline) or the GLUE_GOLD_DATABASE value in `.env` is wrong (should be `edp_dev_gold` with underscores, not hyphens).

**Deploy workflow triggered but ECS task keeps stopping** — Check CloudWatch Logs at `/ecs/edp-dev-analytics-agent` for the startup error. The most common cause is a missing SSM parameter or a container startup crash.

**"Athena query failed: Insufficient permissions on glue:GetDatabase for database/silver"** — dbt-athena embeds the dbt source name (`silver`) as the Glue database in Gold view definitions, not the full name (`edp_dev_silver`). The task role needs Glue read permissions on both `edp_dev_silver` and the literal name `silver`. This is already fixed in the Terraform module.

**"Athena query failed: PERMISSION_DENIED s3:ListBucket on edp-dev-...-silver"** — Gold views read from the underlying Silver Parquet files. The task role needs `s3:GetObject` and `s3:ListBucket` on the Silver S3 bucket. This is already fixed in the Terraform module.

**"Athena: Unable to verify/create output bucket"** — The task role needs `s3:GetBucketLocation` on the Athena results bucket (not just `s3:GetObject`). This is already fixed in the Terraform module.

---

## Deployment

The agent runs as an ECS (Elastic Container Service) Fargate service. It starts automatically when the ECS service is created by Terraform. The `deploy.yml` GitHub Actions workflow builds and deploys a new image on every merge to `main`.

**ECS cluster:** `edp-dev-analytics-agent`
**ECS service:** `edp-dev-analytics-agent`
**ECR repository:** `edp-dev-analytics-agent`
**CloudWatch log group:** `/ecs/edp-dev-analytics-agent`

**HTTP endpoints (reachable from inside the container via ECS Exec):**
```
POST http://localhost:8080/ask
GET  http://localhost:8080/health
```

**ALB DNS (internal, not reachable from the public internet):**
```
internal-edp-dev-agent-alb-753909442.eu-central-1.elb.amazonaws.com
```

The ALB is intentionally internal-only. To reach it from outside the VPC you would need a bastion host or VPN tunnel. For dev testing, use ECS Exec as described in Track 2 above — it's simpler and doesn't require any additional infrastructure.

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
  - s3:PutObject on {gold_bucket}/charts/*

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

### Phase 3: Schema resolver — complete

The Gold layer has 7 small, pre-aggregated tables with 5-10 columns each. All schemas are loaded eagerly at startup and embedded in the system prompt — Claude knows every table and column before it sees the first question. This eliminates the `list_tables` / `get_schema` tool call round trips from the common case and is the single biggest latency saving in the design.

- `agent/schema.py` — `SchemaResolver` class:
  - `load_all_schemas()` — called once at startup. Reads `catalog.json` from `s3://{bronze_bucket}/metadata/dbt/` and fetches all Gold tables from `glue_client.get_tables()`. Merges physical schema (column names, types, partition keys) with business context (column descriptions, accepted values, model docs) for every table. Returns a single dict covering all 7 Gold tables (~2,500 tokens total). This dict is embedded directly in the system prompt so Claude starts every query with full schema awareness.
  - `get_schema(table_name)` — available as a tool for edge cases where Claude needs to re-examine one table during reasoning, but won't be called in normal operation.
  - Graceful fallback if `catalog.json` doesn't exist yet (pipeline hasn't run): falls back to Glue-only schema with a warning logged.
- `tests/test_schema.py` — parametrized tests with full mock fixtures for both Glue and S3 responses, including the fallback path.

Deliverable: `SchemaResolver.load_all_schemas()` returns the complete merged schema for all Gold tables in one call. Tested with and without `catalog.json` present.

### Phase 4: SQL validator — complete

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

### Phase 5: Prompts and Claude client — complete

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

### Phase 6: SQL generator with feedback loop — complete

Gold queries are simple: `SELECT` from one pre-aggregated table with optional `WHERE` filters. A second review pass designed for complex JOINs adds latency and tokens with no benefit here. Single-pass generation with validation feedback is the right design.

- `agent/generator.py` — `SQLGenerator`:
  - Calls `ClaudeClient` with the question. Claude reads the schema from the system prompt and returns a SELECT query and a list of assumptions.
  - Runs the result through `SQLValidator`.
  - If validation fails, sends the error reason back to Claude and asks for a corrected query. Up to 3 attempts before raising `SQLValidationError` to the user.
  - No second review pass — Gold SQL is simple enough that sqlparse guardrail validation is sufficient.
  - Returns validated SQL and flagged assumptions.
- `tests/test_generator.py` — mocked `ClaudeClient`, tests the validation feedback loop, tests assumption extraction.

Deliverable: `SQLGenerator` handles validation failures gracefully and recovers automatically. Single Claude call in the common case.

### Phase 7: Athena executor and cost tracking — complete

Gold tables are small pre-aggregated tables. The worst-case scan cost for any Gold query in a dev environment is a fraction of a cent — complex pre-execution cost estimation via Glue partition enumeration is unnecessary overhead. The Athena WorkGroup `bytes_scanned_cutoff_per_query` setting (configured in the Terraform processing module) is the hard cost backstop. Actual cost is tracked post-execution from the Athena result metadata and recorded in the audit log.

- `agent/executor.py` — `AthenaExecutor`:
  - `execute(sql)` — starts the Athena query, polls until complete, reads the result CSV from the S3 athena-results bucket.
  - Reads `Statistics.DataScannedInBytes` from the completed query execution and converts to USD (~$5 per TB scanned).
  - Returns a pandas DataFrame, actual bytes scanned, and actual cost in USD.
  - Retries on transient Athena errors (throttling, internal service error). Fails immediately on query errors (syntax, permission) with no retry.
- `agent/cost.py` — lightweight utility: one function that converts `DataScannedInBytes` to USD. No Glue calls, no S3 enumeration.
- `tests/test_executor.py` — mocked Athena start/poll/result cycle, failure handling, cost calculation tested.

Deliverable: Full Athena execution path works correctly. Actual cost tracked per query from execution metadata.

### Phase 8: Result validator, insight generator, and audit log — complete

- `agent/result_validator.py` — `ResultValidator`: checks numeric values within plausible bounds (negative revenue is flagged), checks for unexpected nulls on key columns. Zero rows is a valid result for Gold tables — an aggregation with no matching data is a legitimate answer, not a bug. Returns a list of flags, never blocks execution, always surfaces flags in output.
- `agent/insight.py` — `InsightGenerator`: final Claude call that takes the original question, SQL, result DataFrame, and assumptions, and returns a 2-3 sentence plain-English insight. Uses the insight prompt from `prompts.py`. Structured output so malformed responses raise `AgentError`, not crash.
- `agent/audit.py` — `AuditLogger`: writes a structured JSON record to `s3://{bronze_bucket}/metadata/agent-audit/` after every query. Fields: question, SQL, assumptions, row count, bytes scanned, cost in USD, validation flags, insight, timestamp. The audit log is itself queryable via Athena.
- `tests/test_result_validator.py`, `tests/test_insight.py`

### Phase 9: CLI entry point and end-to-end integration — complete

- `agent/main.py` — orchestrates the complete reasoning chain: load schemas → generate SQL → validate → execute → validate results → generate insight → audit log → return output. Handles errors at each stage with clear user-facing messages. CLI entry point: `python -m agent.main "question"`.
- `tests/test_integration.py` — marked `@pytest.mark.integration`, runs against the real AWS dev environment, not mocks. Run manually before deploy, not in CI.

Deliverable: `python -m agent.main "Show total orders by country"` returns SQL, result table, flagged assumptions, and a 2-sentence insight against live Athena data in under 25 seconds.

### Phase 10: Charts — complete

- `agent/charts.py` — `ChartGenerator`:
  - Detects data shape from the DataFrame: time-series, category vs metric, or distribution
  - Time-series → line chart (matplotlib static PNG)
  - 8 or fewer categories → vertical bar chart
  - More than 8 categories → horizontal bar chart sorted by value
  - Uploads PNG to `s3://{gold_bucket}/charts/`, returns presigned URL (valid 1 hour)
  - Plotly interactive HTML version returned in the HTTP endpoint response

### Phase 11: FastAPI HTTP endpoint and session state — complete

- FastAPI route added to `agent/main.py` — POST `/ask` accepts `{"question": "...", "session_id": "..."}`, returns full JSON response: insight, assumptions, validation flags, execution ID, bytes scanned, cost in USD, session ID, chart type, presigned chart URL, interactive HTML chart
- `agent/session.py` — `SessionStore` with TTL eviction (1 hour default). `Conversation.context_summary()` returns the last 5 turns formatted for Claude, enabling multi-turn follow-ups
- GET `/health` returns `{"status": "ok"}` — used by the ALB target group health check

### Phase 12: Deploy pipeline and ECS infra — complete

- `terraform-platform-infra-live/modules/analytics-agent/main.tf` extended with: internal ALB in private subnets, ALB security group (port 80), ECS security group (port 8080 from ALB only), target group with `/health` check, ECS service with rolling deploy and `lifecycle.ignore_changes`
- `.github/workflows/ci.yml` — quality gate (ruff, mypy) + unit tests + Docker build check
- `.github/workflows/deploy.yml` — OIDC (OpenID Connect) authentication, ECR push, ECS task definition update, rolling deploy with stability wait

### Phase 13: Streamlit UI — complete

A Streamlit browser app (`ui/app.py`) that wraps the FastAPI backend so non-technical stakeholders can query the agent without touching a command line.

What was built:

- `ui/app.py` — chat-style text input, conversation history, session state for multi-turn follow-ups
- Result panel: plain-English insight, inline Plotly chart, SQL in a "Query details" expander, assumptions list, scan cost and bytes scanned
- "Send as email" form: generates a PDF report (question + chart + insight) and sends it via AWS SES
- Runs in the same ECS Fargate container as FastAPI. `entrypoint.sh` starts uvicorn on port 8080 (background) then Streamlit on port 8501 (foreground)
- ALB has separate listeners for port 80 (FastAPI) and port 8501 (Streamlit)
- Deployed automatically via the existing CI/deploy pipeline on every push to main

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
| ALB (3 hours) | ~$0.05 |
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
| FastAPI | HTTP endpoint for the agent API |
| Streamlit | Python library that turns Python code into a browser-accessible web page — the stakeholder UI |
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
│   ├── session.py              ← SessionStore + Conversation: multi-turn context management
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
│   ├── test_session.py
│   ├── test_charts.py
│   └── test_integration.py     ← marked @pytest.mark.integration, runs against real AWS dev
├── ui/                         ← Streamlit browser UI (Phase 13)
│   └── app.py                  ← the web page: question input, insight display, chart rendering
├── .python-version             ← 3.11.8 (pyenv)
├── pyproject.toml              ← ruff, mypy, pytest config
├── requirements.txt            ← runtime dependencies
├── requirements-dev.txt        ← dev tools: ruff, mypy, pytest
├── entrypoint.sh               ← starts FastAPI (port 8080) and Streamlit (port 8501) together
├── Dockerfile                  ← two-stage build, non-root user
├── docker-compose.yml          ← local dev
├── Makefile                    ← setup, lint, typecheck, test, run
└── README.md                   ← this file
```

---

## How the tests work

The agent calls real AWS services (S3, Athena, SSM, Glue) and the Claude API. If the tests made those real calls on every push, they would cost money, be slow, and require valid credentials in CI. Instead, the tests intercept those calls and return fake responses from memory. No real network traffic happens at all.

Two tools make this possible.

**moto** pretends to be AWS. When the application code calls `boto3.client("s3").put_object(...)`, moto catches that call and stores the data in a Python dictionary in RAM. The code never knows it isn't talking to real S3. When the test finishes, everything is discarded.

**unittest.mock** replaces any Python function with a fake version. When the code calls the Claude API to generate SQL, mock substitutes that function with one that instantly returns a hardcoded string like `SELECT * FROM revenue_by_country LIMIT 10`. No HTTP request, no API key needed.

There are no CSV test data files because the agent doesn't read CSV files — it reads AWS API responses and Claude API responses. Mocking those responses directly is more accurate than representing them as CSV, and the mocks stay in sync with the code automatically.

```mermaid
flowchart TD
    A[Push to GitHub] --> B[Four jobs run in parallel]
    B --> C[Lint\nruff checks code style]
    B --> D[Type check\nmypy checks type correctness]
    B --> E[Unit tests\nmoto replaces AWS calls\nunittest.mock replaces Claude API\nNo real network calls made]
    B --> F[Docker build\nVerifies image builds cleanly]
    C --> G{All four pass?}
    D --> G
    E --> G
    F --> G
    G -->|No| H[Pipeline stops here\nDeploy workflow is skipped]
    G -->|Yes| I[Deploy workflow triggers\nBuilds Docker image\nPushes to ECR\nTriggers ECS rolling deploy]
```

Integration tests also exist but are not part of the standard CI run. They are marked `@pytest.mark.integration` and only run when explicitly triggered with real AWS credentials against the deployed dev environment. They validate the full pipeline end-to-end: real Glue schema loading, real Athena query, real Claude API call.

---

## Status

All 13 phases complete. The full agent is deployed to ECS Fargate on AWS dev and end-to-end tested: non-technical stakeholders open a browser, type a plain-English question, and see the insight, interactive Plotly chart, SQL, assumptions, and scan cost. PDF reports can be sent by email via AWS SES.

| Phase | Status |
|---|---|
| 1: Foundation (skeleton, CI, Docker, exceptions, config, logging) | Complete |
| 2: IAM and infra (ECR, ECS cluster, task role, task definition) | Complete |
| 3: Schema resolver (Glue + dbt catalog.json → system prompt) | Complete |
| 4: SQL validator (sqlparse guardrails, SELECT-only, Gold DB only) | Complete |
| 5: Prompts and Claude client (single-call path, tool-use fallback, retry) | Complete |
| 6: SQL generator with feedback loop (3-attempt validation retry) | Complete |
| 7: Athena executor and cost tracking (poll, read results, DataScannedInBytes) | Complete |
| 8: Result validator, insight generator, audit log | Complete |
| 9: CLI entry point and end-to-end integration | Complete |
| 10: Charts (matplotlib PNG, Plotly HTML, S3 presigned URL) | Complete |
| 11: FastAPI HTTP endpoint and session state (multi-turn follow-ups) | Complete |
| 12: Deploy pipeline (OIDC, ECR push, ECS rolling deploy, ALB, ECS service) | Complete |
| 13: Streamlit UI (browser interface, same-container deploy, ALB port 8501) | Complete |

This is the last component of the platform. The full pipeline is: PostgreSQL → DMS CDC → Bronze S3 → Glue PySpark → Silver S3 → dbt/Athena → Gold S3 → Redshift Serverless → this agent.

---

**Full platform overview:** [platform-docs](https://github.com/enterprise-data-platform-emeka/platform-docs) has the complete build guide, architecture diagrams, design decisions, and step-by-step instructions for deploying the entire platform from scratch.
