# Healthcare Insurance Claims Analytics & Fraud Signal Platform

A hybrid lakehouse-plus-warehouse data platform built on fully synthetic healthcare claims data, demonstrating end-to-end data engineering across Kafka, Spark, Delta Lake, dbt, Airflow, Terraform, and Snowflake.

![Delta Lake](https://img.shields.io/badge/Delta%20Lake-lakehouse-blue)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red)
![Snowflake](https://img.shields.io/badge/Snowflake-serving%20layer-29B5E8)
![dbt](https://img.shields.io/badge/dbt-transformation-FF694B)
![Airflow](https://img.shields.io/badge/Airflow-orchestration-017CEE)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-compatible-231F20)

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Key Architectural Decisions](#key-architectural-decisions)
- [Problems Encountered and How They Were Resolved](#problems-encountered-and-how-they-were-resolved)
- [Fraud Detection Results](#fraud-detection-results)
- [Lessons Learned](#lessons-learned)
- [Snowflake Lifecycle Note](#snowflake-lifecycle-note)

---

## Architecture

```
Kafka (Redpanda) → Spark → Delta Lake (Bronze → Silver → Gold)
                              ↓
                    Snowflake (Serving Layer)
                              ↓
                    dbt (serving_marts) → Plotly Dashboard
```

Orchestrated end-to-end by Airflow. Infrastructure (Databricks schemas/jobs, Snowflake warehouse/database/schema/roles) provisioned via Terraform.

## Tech Stack

| Layer | Tool | Role |
|---|---|---|
| Streaming | Redpanda (Kafka-compatible) | Claim event ingestion |
| Processing | PySpark (local + Databricks) | Batch and streaming transforms |
| Lakehouse | Delta Lake on Databricks Free Edition | Bronze/Silver/Gold medallion tiers |
| Warehouse | Snowflake (trial) | Serving layer for BI |
| Transformation | dbt (two projects) | `lakehouse_transform` (Databricks), `serving_marts` (Snowflake) |
| Orchestration | Apache Airflow (Docker Compose) | End-to-end scheduling |
| IaC | Terraform | Databricks schemas/jobs, Snowflake warehouse/db/schema/roles |
| BI | Plotly | Static + live HTML dashboard |

## Repository Structure

```
claims-lakehouse-platform/
├── docker-compose.yml
├── terraform/
│   ├── databricks/
│   └── snowflake/
├── data_generator/
├── spark_jobs/
├── dbt/
│   ├── lakehouse_transform/
│   └── serving_marts/
├── airflow/
│   └── dags/
├── dashboard/
│   └── build_dashboard.py
├── sample_data/
└── README.md
```

---

## Key Architectural Decisions

**1. Hybrid lakehouse + warehouse, not one or the other.** Delta Lake (open format, free-forever compute on Databricks Free Edition) holds raw/refined tiers; Snowflake serves as the governed, fast-query serving layer — chosen to demonstrate both the medallion pattern and warehouse serving trade-offs in a single project.

**2. Snowflake trial deliberately delayed to Week 11.** Snowflake trials are 30 days or $400 credit, whichever comes first, and can't be extended or renewed into the same account. Sequencing all Databricks/Delta/dbt/Airflow fundamentals first (free-forever) meant the trial window was spent entirely on the serving layer and final integration/demo.

**3. Redpanda instead of vanilla Kafka.** Kafka-API-compatible, single binary, no Zookeeper — reduces local setup friction on Windows without sacrificing the underlying concepts.

**4. Two separate dbt projects, not one.** dbt materializes against a single compute engine per project. `lakehouse_transform` targets Databricks/Delta Lake; `serving_marts` targets Snowflake — a real constraint in dbt's design, not an arbitrary split.

**5. Terraform added deliberately as a sixth tool**, despite the added time cost, specifically for the infrastructure-as-code interview story. Scope was adjusted to what Databricks Free Edition actually supports (see Problem #6 below).

**6. Kafka sequenced last (Week 12), not mid-project.** Spark's Bronze ingest logic was built and validated against a static JSONL stub first; the live Kafka producer replaced the stub only once the consumer-side transformation logic was already proven.

**7. Gold → Snowflake handoff via batch export + `COPY INTO`**, not external tables over the Delta log. Chosen for simplicity and query performance, at the cost of one-time data duplication — within reach of Free Edition's storage limitations (see Problem #10).

---

## Problems Encountered and How They Were Resolved

Documented deliberately — a pipeline that never broke would be a less credible portfolio piece than one with real, diagnosed failures.

<details>
<summary><strong>1. YAML folding bug in docker-compose.yml (Week 0/5-6)</strong></summary>

A multi-line `command: >` block, indented for visual alignment, broke YAML's folded-scalar rules — extra indentation caused literal newlines to survive instead of being folded into spaces, so Bash received a multi-line string and interpreted the second line as a separate, invalid command. Fixed by collapsing to a single-line `command:` string.
</details>

<details>
<summary><strong>2. Unity Catalog prompt during dbt init (Week 3-4)</strong></summary>

Databricks has moved to Unity Catalog as the standard; `dbt-databricks` now prompts for a catalog explicitly. Required creating a catalog manually in the Databricks UI first, then correcting `profiles.yml`'s `catalog:` field (initially and incorrectly set to the legacy `hive_metastore`).
</details>

<details>
<summary><strong>3. Missing gold schema (Week 7-9)</strong></summary>

Writing to `gold.provider_billing_summary` failed — the `gold` schema was never created, unlike `bronze`/`silver`. Fixed with an explicit `CREATE SCHEMA IF NOT EXISTS`, later formalized as a Terraform-managed `databricks_schema` resource in Week 10.
</details>

<details>
<summary><strong>4. Catalog inconsistency: workspace vs. claims_platform (Week 7-9)</strong></summary>

dbt-built tables landed under the default `workspace` catalog instead of `claims_platform`, while Spark-written Bronze tables landed correctly. Root cause: `profiles.yml`'s catalog setting wasn't consistently applied across all dbt run contexts (local vs. Airflow-mounted copy).
</details>

<details>
<summary><strong>5. NameError on a missing PySpark import (Week 7-9)</strong></summary>

Missing `from pyspark.sql.functions import sum as _sum` in a fresh notebook session. The alias itself exists to avoid shadowing Python's built-in `sum()`.
</details>

<details>
<summary><strong>6. Terraform + Databricks Free Edition compute limitation (Week 10)</strong></summary>

Free Edition is serverless-only — the `databricks_cluster` Terraform resource, originally planned for "provision a cluster as code," cannot be used at all. Pivoted scope to Terraform-managed schemas and a job defined with no cluster block (omitting compute signals serverless) — both Free-Edition-compatible and arguably a stronger, more current interview story.
</details>

<details>
<summary><strong>7. Terraform "already exists" errors on schema creation (Week 10)</strong></summary>

`bronze`, `silver`, and `gold` schemas already existed from earlier manual creation; `apply` tried to recreate them and failed. Resolved via `terraform import` for each schema, bringing pre-existing infrastructure under management rather than recreating it.
</details>

<details>
<summary><strong>8. Import brought in unmanaged properties as drift (Week 10)</strong></summary>

After import, `terraform plan` proposed removing system-managed properties (`collation`, `owner`, an internal Iceberg/Delta interop flag) never declared in the `.tf` config. Root cause: Terraform treats an omitted map-typed field as "should be empty," not "leave alone" — a known gotcha when importing existing infrastructure. Verified the removal was safe before applying.
</details>

<details>
<summary><strong>9. Job resource silently never applied (Week 10)</strong></summary>

`terraform state list` showed only the schemas — the job resource had never been through `apply`, because the notebook its `notebook_task` pointed to didn't exist yet. Fixed by creating the notebook first, then applying.
</details>

<details>
<summary><strong>10. Free Edition has no direct cloud storage bridge to Snowflake (Week 11)</strong></summary>

Gold data couldn't be handed to Snowflake via `COPY INTO ... FROM s3://...` as it would on a paid workspace. Worked around with a manual export (`.toPandas().to_csv()`) → download → SnowSQL `PUT`/`COPY INTO` sequence — documented explicitly as a Free-Edition-specific workaround, not the production pattern (which would use cloud storage + Snowpipe).
</details>

<details>
<summary><strong>11. SnowSQL session context errors, twice, for different reasons (Week 11)</strong></summary>

First: `PUT` failed with "no current database" — a fresh SnowSQL session doesn't inherit context from a separate Snowsight browser session. Second: a "stage does not exist" error with a genuinely different cause — the target table itself hadn't been created in that session, so its implicit table-stage didn't exist either.
</details>

<details>
<summary><strong>12. Windows file path resolution for SnowSQL PUT (Week 11)</strong></summary>

`file://relative_path` resolved against the CLI's launch directory, not the CSV's actual location. Fixed using an absolute path with forward slashes, even on Windows.
</details>

<details>
<summary><strong>13. Column count mismatch on COPY INTO (Week 11)</strong></summary>

The table DDL was written for a 7-column dbt mart, but the exported CSV came from an unrelated 2-column ad hoc Spark tuning exercise — two different tables had been conflated. Resolved by exporting and loading the correct, richer mart with the actual fraud-signal columns.
</details>

<details>
<summary><strong>14. Fraud-detection false positive investigation (Week 11)</strong></summary>

The `is_billing_spike` rule flagged 3 providers; only 2 were seeded as fraudulent. Ground truth was confirmed directly from the data (`WHERE billed_amount > 5000`, since no legitimate procedure exceeds that) rather than attempting to regenerate the original random selection, which turned out to be non-reproducible (`uuid.uuid4()` draws from OS entropy, unaffected by `random.seed()`). Result: 2/2 true positives, 1 false positive — root-caused to a legitimately expensive procedure landing on an otherwise normal day for that provider.
</details>

<details>
<summary><strong>15. Kafka at-least-once delivery produced duplicates (Week 12)</strong></summary>

Bronze ended up with 62,839 rows for 50,000 actual claims after a streaming run. Root cause: Kafka topics don't deduplicate by key by default, and the topic likely retained messages from an earlier producer run combined with `startingOffsets="earliest"`. Resolved without any code change — the Silver-layer `row_number()` dedup logic, built weeks earlier, collapsed Bronze's rows to the correct 50,000 distinct claims automatically.
</details>

<details>
<summary><strong>16. Local Kafka unreachable from Databricks compute (Week 12)</strong></summary>

A Structured Streaming job referencing `localhost:19092` can't resolve correctly on remote Databricks serverless compute — `localhost` there refers to the executor itself. A control-plane vs. data-plane distinction: Databricks authentication has no bearing on network reachability to a broker in local Docker. Resolved by validating the full producer→consumer path locally, then batch-appending the result into the real Unity Catalog table via the existing authenticated connection.
</details>

<details>
<summary><strong>17. Snowflake private key needed a volume mount, not an env var (Week 13)</strong></summary>

Unlike simple string secrets, the Snowflake connector's `private_key_file` parameter expects a filesystem path — so the PEM key had to be mounted into the Airflow container via Docker Compose volumes, referenced at a fixed container-internal path separate from its location on the Windows host.
</details>

---

## Fraud Detection Results

A simple 3x-rolling-7-day-average threshold (`is_billing_spike`, built in dbt) was tested against synthetic data with 2 deliberately seeded fraud-pattern providers (4-6x billing multiplier on ~15% of their claims).

| Metric | Result |
|---|---|
| True positives | 2 / 2 |
| False positives | 1 |
| Recall | 100% |
| Precision | 67% |

The false positive is explainable and documented (Problem #14) — a legitimate high-cost procedure landing on an otherwise typical day. A more robust version would condition the threshold on procedure-mix composition rather than raw dollar totals, or require a spike to persist across multiple days before flagging.

---

## Lessons Learned

- **Bronze/Silver/Gold separation isn't theoretical** — it transparently absorbed a real Kafka duplicate-delivery event with zero manual intervention.
- **Free Edition's serverless-only model** shapes real architectural decisions (Terraform scope, Kafka-Databricks networking) — documented as deliberate workarounds rather than gaps in understanding.
- **`terraform plan` before `apply` is non-negotiable**, especially after `import` — omitted config fields are treated as "should not exist," not "leave alone."
- **Control-plane authentication ≠ data-plane network reachability** — a generally useful distributed-systems distinction, encountered concretely via the Kafka/Databricks networking gap.
- **A clean, zero-false-positive fraud model would have been a red flag, not a strength** — the documented false positive and its root cause are stronger evidence of real understanding than a suspiciously perfect result.

---

## Snowflake Lifecycle Note

The Snowflake serving layer was provisioned and demonstrated during a 30-day trial window (Weeks 11-13) and is not expected to remain live indefinitely. Durable proof of its output is preserved in `dashboard/claims_dashboard.html` and `sample_data/`. The Databricks Free Edition / Delta Lake lakehouse layer has no expiry and remains fully live and reproducible.