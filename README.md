# Healthcare Insurance Claims Analytics & Fraud Signal Platform

A hybrid lakehouse-plus-warehouse data platform built on fully synthetic healthcare claims data, demonstrating production-grade data engineering patterns across streaming ingestion, distributed processing, transactional lakehouse storage, SQL-based transformation, orchestration, infrastructure-as-code, and cloud data warehousing.

![Delta Lake](https://img.shields.io/badge/Delta%20Lake-lakehouse-blue)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red)
![Snowflake](https://img.shields.io/badge/Snowflake-serving%20layer-29B5E8)
![dbt](https://img.shields.io/badge/dbt-transformation-FF694B)
![Airflow](https://img.shields.io/badge/Airflow-orchestration-017CEE)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda--compatible-231F20)
![PySpark](https://img.shields.io/badge/PySpark-distributed%20compute-E25A1C)

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Data Model](#data-model)
- [Key Architectural Decisions](#key-architectural-decisions)
- [Technical Deep Dives](#technical-deep-dives)
  - [Delta Lake: Transactional Guarantees on Object Storage](#delta-lake-transactional-guarantees-on-object-storage)
  - [Spark: Distributed Processing and Performance Engineering](#spark-distributed-processing-and-performance-engineering)
  - [dbt: Transformation as Software Engineering](#dbt-transformation-as-software-engineering)
  - [Airflow: Orchestration and Failure Handling](#airflow-orchestration-and-failure-handling)
  - [Snowflake: Elastic Warehouse Architecture](#snowflake-elastic-warehouse-architecture)
  - [Kafka: Streaming Ingestion and Delivery Semantics](#kafka-streaming-ingestion-and-delivery-semantics)
  - [Terraform: Infrastructure as Code](#terraform-infrastructure-as-code)
- [Performance and Optimization Notes](#performance-and-optimization-notes)
- [Major Problems Encountered](#major-problems-encountered)
- [Fraud Detection Methodology and Results](#fraud-detection-methodology-and-results)
- [Engineering Retrospective](#engineering-retrospective)
- [Future Enhancements](#future-enhancements)
- [Getting Started](#getting-started)
- [Snowflake Lifecycle Note](#snowflake-lifecycle-note)

---

## Overview

This project simulates a healthcare insurance claims pipeline — from raw claim event ingestion through to a fraud-signal-flagged, BI-ready serving layer — using synthetic data designed to resemble real claims adjudication patterns without any PHI/PII exposure risk. The goal was to build a system that a data platform team would recognize as structurally sound, not a toy pipeline: proper medallion layering, tested transformations, orchestrated scheduling, infrastructure managed as code, and a documented, honestly-reported fraud detection result rather than an inflated one.

The platform ingests claim lifecycle events, processes them through a Spark-based transformation layer into a Delta Lake medallion architecture, surfaces business-level aggregates into a Snowflake serving layer via a governed ELT handoff, and exposes fraud signals through a tested dbt mart and a Plotly dashboard — all orchestrated end-to-end by Airflow.

---

## Architecture

```
┌─────────────┐     ┌───────────┐     ┌──────────────────────────────────┐
│   Kafka     │ ──▶ │   Spark   │ ──▶ │      Delta Lake (Databricks)      │
│ (Redpanda)  │     │  (batch + │     │  Bronze → Silver → Gold tiers     │
│             │     │ streaming)│     │  dbt project: lakehouse_transform │
└─────────────┘     └───────────┘     └────────────────┬───────────────────┘
                                                        │  batch export
                                                        │  (Parquet/CSV → COPY INTO)
                                                        ▼
                                         ┌──────────────────────────────────┐
                                         │           Snowflake               │
                                         │  Serving layer (SERVING schema)   │
                                         │  dbt project: serving_marts       │
                                         └────────────────┬───────────────────┘
                                                          │
                                                          ▼
                                                 ┌──────────────────┐
                                                 │  Plotly Dashboard │
                                                 └──────────────────┘

         Orchestrated end-to-end by Apache Airflow (Docker Compose, LocalExecutor)
         Infrastructure provisioned by Terraform (Databricks + Snowflake providers)
```

This is a deliberate **hybrid lakehouse-plus-warehouse** design, not a single-paradigm pipeline. The lakehouse tier (Delta Lake on Databricks) handles raw ingestion, transformation, and business logic on an open, ACID-compliant format with zero ongoing cost. The warehouse tier (Snowflake) exists purely as a fast, governed serving layer for BI consumption — the two are connected by a deliberate, documented ELT handoff rather than either system trying to do the other's job.

---

## Tech Stack

| Layer | Tool | Role | Why This Tool |
|---|---|---|---|
| Streaming ingestion | Redpanda (Kafka API-compatible) | Claim event ingestion | Kafka-protocol compatible, single binary, no Zookeeper dependency — reduces local setup overhead without sacrificing the underlying pub/sub concepts |
| Distributed processing | PySpark (local + Databricks) | Batch and streaming transforms | Out-of-core, cluster-parallel processing; industry-standard for large-scale ETL |
| Lakehouse storage | Delta Lake on Databricks Free Edition | Bronze/Silver/Gold medallion tiers | ACID transactions and time travel on top of open Parquet files, at zero ongoing infrastructure cost |
| Cloud warehouse | Snowflake | Serving layer for BI | Storage/compute separation, elastic virtual warehouses, strong governance model |
| Transformation | dbt (two independent projects) | SQL-based modeling, testing, documentation, lineage | Brings software engineering discipline (version control, testing, DAG-based dependency resolution) to SQL transformations |
| Orchestration | Apache Airflow (Docker Compose, LocalExecutor) | End-to-end scheduling, retries, alerting | DAG-based dependency modeling, backfill support, first-class failure handling — the standard against which pipeline schedulers are measured |
| Infrastructure as Code | Terraform (Databricks + Snowflake providers) | Declarative provisioning of schemas, jobs, warehouses, roles, grants | Reproducible, auditable, version-controlled infrastructure instead of manual console configuration |
| BI / visualization | Plotly | Dashboard layer | Python-native, exports to fully static, dependency-free HTML — durable even after backing infrastructure is decommissioned |

---

## Repository Structure

```
claims-lakehouse-platform/
├── docker-compose.yml          # Airflow (LocalExecutor) + Redpanda, locally orchestrated
├── terraform/
│   ├── databricks/             # Schema + job provisioning (serverless-compatible)
│   └── snowflake/               # Warehouse, database, schema, role, grant provisioning
├── data_generator/
│   └── generate_claims.py      # Deterministic synthetic claims generator with seeded fraud patterns
├── spark_jobs/
│   ├── bronze_ingest.py                 # Batch Bronze ingest (static file source)
│   ├── bronze_ingest_streaming.py       # Structured Streaming Bronze ingest (Kafka source)
│   └── kafka_producer.py                # Replays synthetic claims as a live event stream
├── dbt/
│   ├── lakehouse_transform/    # dbt project targeting Databricks/Delta Lake (Bronze → Silver → Gold)
│   └── serving_marts/          # dbt project targeting Snowflake (serving-layer marts + tests)
├── airflow/
│   └── dags/
│       └── full_pipeline_dag.py  # End-to-end DAG spanning both dbt projects, the Snowflake load, and the dashboard
├── dashboard/
│   └── build_dashboard.py      # Queries serving_marts, generates a static Plotly HTML dashboard
├── docs/                       # Query profile screenshots, dbt lineage graphs
├── sample_data/                # Exported Parquet/CSV snapshots for post-trial portfolio durability
└── README.md
```

---

## Data Model

**Bronze — `bronze.claims_submitted`** (Delta Lake, partitioned by `submitted_date`)
Raw, as-ingested claim events. No transformation, deduplication, or validation applied. Populated by both a batch job (static-file source) and a Structured Streaming job (Kafka source) sharing identical downstream schema and partitioning strategy.

| Column | Type | Notes |
|---|---|---|
| `claim_id` | STRING | Not guaranteed unique at this tier — duplicates from at-least-once delivery are expected and retained |
| `patient_id` | STRING | |
| `provider_id` | STRING | |
| `procedure_code` | STRING | |
| `procedure_desc` | STRING | |
| `billed_amount` | DOUBLE | Un-cast; precision issues expected and corrected downstream |
| `submitted_at` | STRING (raw) | Cast to timestamp in Silver |
| `submitted_date` | DATE | Derived at ingest time; used as the partition column |

**Silver — `silver.stg_claims__submitted`** (dbt model, view materialization)
Deduplicated on `claim_id` (most recent `submitted_at` wins via `row_number()`), type-cast (`billed_amount` → `decimal(10,2)`), whitespace-trimmed identifiers, and filtered to exclude malformed rows (null keys, non-positive billed amounts).

**Gold — `silver.dim_providers`, `silver.fct_provider_daily_billing`** (dbt models, table materialization)
- `dim_providers`: one row per provider, lifetime claim/billing aggregates.
- `fct_provider_daily_billing`: daily billing rollup per provider, with a `rolling_7day_avg_billed` window calculation and a boolean `is_billing_spike` flag (day's average > 3x trailing 7-day average) — the core fraud signal.

**Snowflake serving layer — `CLAIMS_PLATFORM.SERVING.mart_fraud_signals`** (dbt model, `serving_marts` project)
Loaded from the Gold tier via batch export + `COPY INTO`, then re-modeled with a normalized `spike_ratio` column (`avg_billed_amount / rolling_7day_avg_billed`) for dashboard consumption — more interpretable than a raw boolean for a BI audience.

---

## Key Architectural Decisions

**1. Hybrid lakehouse + warehouse, not one or the other.**
Delta Lake (open format, free-forever compute on Databricks Free Edition) holds raw and refined tiers; Snowflake serves purely as the governed, fast-query serving layer. This mirrors a real, current industry pattern — many mid-size organizations are converging on exactly this split, using the lakehouse for flexible, engine-agnostic storage and a warehouse for BI-facing serving where query simplicity and governance matter more than storage format openness.

**2. Snowflake trial deliberately delayed to Week 11 of a 13-week build.**
Snowflake trial accounts run 30 days or until a fixed credit balance is exhausted, whichever comes first, and cannot be renewed into the same account. Every tool with no such constraint (Databricks Free Edition, dbt, Airflow, Spark, Kafka/Redpanda) was learned and built first; the trial window was spent entirely on the serving layer, the Terraform-provisioned warehouse, and final end-to-end integration — the highest-value window for a resource with a hard expiry.

**3. Redpanda instead of vanilla Apache Kafka.**
Kafka-API-compatible, ships as a single binary with no Zookeeper dependency, and runs cleanly in Docker on Windows. Chosen specifically to minimize local infrastructure friction without sacrificing any of the streaming concepts (partitions, consumer groups, delivery semantics, offsets) that transfer directly to a production Kafka deployment.

**4. Two independent dbt projects, not one spanning both engines.**
dbt materializes every model against a single configured compute engine per project — there is no way for one dbt project to span both Databricks and Snowflake simultaneously. `lakehouse_transform` targets Databricks/Delta Lake for Bronze → Silver → Gold; `serving_marts` targets Snowflake for the final BI-facing layer. This is a direct consequence of dbt's architecture, and the split is documented as such rather than presented as an arbitrary organizational choice.

**5. Terraform added deliberately, with scope adjusted to platform constraints.**
Terraform was added specifically for the infrastructure-as-code interview narrative, provisioning Databricks schemas/jobs and the full Snowflake object hierarchy (warehouse, database, schema, role, grants) declaratively rather than via console clicks. Scope was adjusted mid-project once Databricks Free Edition's serverless-only constraint was discovered (see Major Problems Encountered) — the final Terraform surface reflects what the platform tier actually supports, not the original, more ambitious plan.

**6. Kafka sequenced last in the build order, not introduced early.**
The Bronze ingest transformation logic (schema, partitioning strategy, target table) was built and fully validated against a static, deterministic JSONL file first. The live Kafka producer and Structured Streaming consumer were introduced only once that logic was proven — a deliberate "validate against a stable input before adding a less predictable upstream" sequencing decision, common in real pipeline development.

**7. Gold-to-Snowflake handoff via batch export + `COPY INTO`, not external tables over the Delta transaction log.**
Snowflake can read Delta-formatted files directly via external tables referencing the Delta log, avoiding data duplication entirely. This project instead uses a batch export (Parquet/CSV) followed by a native `COPY INTO` load — trading one-time data duplication for significantly better query performance and full compatibility with Snowflake's native micro-partition pruning and clustering. This is also the pattern available within Databricks Free Edition's storage constraints, which lacks a bring-your-own-cloud-storage option that would enable the external-table alternative.

---

## Technical Deep Dives

### Delta Lake: Transactional Guarantees on Object Storage

Plain Parquet files in object storage have no native concept of a transaction: concurrent writers or a failed mid-write can leave a folder in an inconsistent, uninterpretable state. Delta Lake solves this with a single core mechanism — a JSON transaction log (`_delta_log/`) sitting alongside the data files, which is the sole source of truth for what the table's current, valid state is.

Every write operation — an insert, an update, an `OPTIMIZE` compaction — produces a new, numbered JSON commit recording exactly which Parquet files were added and which were logically removed. Critically, **existing Parquet files are never edited in place**: an update rewrites affected files and marks the originals as removed in the log, rather than mutating them. This single design choice is what enables:

- **Atomicity** — a reader only ever sees the last fully-committed log entry, never a partial write.
- **Time travel** — since removed files aren't immediately deleted, querying `VERSION AS OF n` or a historical timestamp simply replays the log up to that point.
- **Schema enforcement** — the log tracks schema per version, so an incompatible write fails loudly rather than silently corrupting downstream readers.
- **Optimistic concurrency** — writers attempt to claim the next sequential commit number; a collision causes a retry rather than requiring a lock.

This project's Bronze tier makes this concrete and verifiable: `DESCRIBE HISTORY` and direct inspection of `_delta_log/*.json` files were used to confirm, empirically, that an `UPDATE` produced a new versioned commit rather than mutating existing files, and that `VERSION AS OF 0` correctly reconstructed pre-update state.

**Compaction and file layout.** Frequent small writes — especially from a streaming source — produce many small Parquet files over time, which hurts read performance due to per-file open/metadata overhead. `OPTIMIZE` rewrites a table's files into fewer, well-sized ones (targeting roughly 1GB each); `ZORDER BY` goes further, physically co-locating rows with similar values of a frequently-filtered column, which combined with Delta's per-file min/max statistics dramatically improves file-skipping for selective queries — the lakehouse-native analog to a traditional database index.

### Spark: Distributed Processing and Performance Engineering

Spark's core value proposition over single-node tools like Pandas is distributing computation across a cluster and processing data that doesn't fit in memory on one machine. Understanding *where the cost actually lives* in a Spark job is the difference between "used PySpark" and genuine performance engineering.

**Shuffles.** Any wide transformation — `groupBy`, a non-broadcast `join`, `distinct`, `orderBy` — requires physically redistributing data across the cluster so that rows sharing a key land on the same executor. This is the single most expensive operation category in Spark, involving serialization, network transfer, and often disk spill. Reading `.explain(mode="formatted")` output and correctly identifying a shuffle boundary (`Exchange` in vanilla Spark; `PhotonShuffleExchangeSink`/`Source` on Databricks' native Photon engine) is a directly practical debugging skill, not just terminology — this project used it to confirm exactly when and why a `groupBy("provider_id")` aggregation triggered data movement.

**Partial aggregation.** Spark's query planner automatically splits aggregations into a partition-local partial aggregation followed by a final aggregation after the shuffle — verified in this project's execution plans as two distinct `GroupingAgg` steps around the shuffle boundary. This reduces shuffle volume from "every raw row" to "one partial summary row per key per partition," which matters regardless of the underlying row count.

**Broadcast joins.** For a join against a small dimension table (here, `dim_providers` at ~40 rows against a much larger fact table), forcing a broadcast join (`broadcast(small_df)`) sends the small table to every executor rather than shuffling both sides — eliminating the large table's shuffle entirely. Spark applies this automatically under a configurable size threshold, but making it explicit is both a clarity practice and a safety net.

**Partition strategy and shuffle-partition tuning.** Bronze is partitioned by `submitted_date` — chosen deliberately for moderate cardinality (roughly 90 distinct values across the dataset's date range) combined with being a commonly-filtered column, avoiding the over-partitioning anti-pattern that a high-cardinality column like `claim_id` would produce. Separately, `spark.sql.shuffle.partitions` (a fixed, cluster-scale-agnostic default of 200) was tuned down for this dataset's actual volume, preventing the small-file problem at write time rather than requiring `OPTIMIZE` to clean it up after the fact — with Adaptive Query Execution noted as the more sophisticated, size-aware modern alternative to manual tuning.

### dbt: Transformation as Software Engineering

dbt doesn't add SQL capability — every model here is SQL that could be run directly. What it adds is **engineering discipline around SQL**: version control, automatic dependency resolution via `ref()`/`source()`, testing, and generated documentation and lineage.

**Automatic DAG construction.** Every `{{ ref('model_name') }}` call is both a query dependency and a signal dbt uses to build an execution DAG — `dbt run` builds models in the correct order without any manually-specified orchestration, purely by parsing which models reference which others.

**Materialization strategy as a real performance/cost trade-off.** Staging models are materialized as views (no storage duplication, always current, cheap since they're thin passthrough logic queried infrequently); mart-layer models are materialized as tables (faster for repeated downstream/BI queries, at the cost of needing an explicit rebuild to reflect new data). This project applies that distinction deliberately rather than defaulting one way throughout.

**Testing as an automated data-quality gate.** `not_null`/`unique` tests catch the class of bug that otherwise surfaces three layers downstream as an unexplained wrong number; `relationships` tests serve as dbt's equivalent of a foreign key constraint, which neither Delta Lake nor Spark enforce natively; range/accepted-value tests catch quiet data drift. These tests run as part of the orchestrated Airflow DAG, not as a manual, easily-skipped step.

### Airflow: Orchestration and Failure Handling

Airflow's value over a simple cron schedule is dependency-aware scheduling with first-class failure handling — this project's DAGs encode both.

**Retries with exponential backoff.** Transient failures (a Databricks SQL warehouse still cold-starting, a brief network blip) are handled with automatic retries on an exponentially increasing delay, capped at a maximum — avoiding both premature failure on a recoverable blip and hammering a struggling downstream dependency with immediate, repeated retries.

**Execution timeouts.** Protects against a silently-hung task (e.g., a compute resource that never responds) occupying a worker slot indefinitely without ever triggering a failure alert — a timeout converts an indefinite hang into an honest, alertable failure.

**Failure callbacks over dashboard-polling.** Alerting is attached at the task level via `on_failure_callback`, firing automatically the moment a task exhausts its retries — rather than depending on a human discovering a failure by checking a UI.

**Task granularity for observability.** `dbt run` and `dbt test` are modeled as separate tasks rather than one combined step specifically so a failure's cause (a model failing to build vs. a test failing against data that built successfully) is visible immediately from the DAG's graph view, without digging through combined logs.

### Snowflake: Elastic Warehouse Architecture

Snowflake's foundational architectural decision — separating storage from compute — is the answer to nearly every "why Snowflake" interview question, and this project's provisioning reflects it directly.

**Storage**: data lives as immutable, compressed micro-partitions (50-500MB each) with Snowflake automatically tracking per-column min/max statistics per partition — enabling **pruning**, where a selective query skips scanning partitions that can't contain matching rows, without any manually-built index. This project's query profile artifacts (see `docs/`) directly capture partitions-scanned-vs-total for a date-filtered query against an unfiltered baseline.

**Compute**: independent, elastically-sized virtual warehouses that can scale, suspend, and resume without any coupling to the underlying storage — provisioned here at `XSMALL` with `auto_suspend`/`auto_resume` configured explicitly, since an idle warehouse otherwise consumes trial credits for no benefit.

**Authentication and least privilege.** Provisioning uses key-pair (JWT) authentication rather than password auth — the private key never crosses the network, since authentication works via a signed challenge rather than a transmitted secret. Terraform operates as `SYSADMIN`, not `ACCOUNTADMIN`, scoping the blast radius of any provisioning mistake away from billing/account-level settings; a separate analyst role receives only the specific `USAGE` grants it needs.

### Kafka: Streaming Ingestion and Delivery Semantics

**Delivery guarantees.** Kafka's default delivery model is **at-least-once** — this project encountered a live instance of this directly: a Structured Streaming consumer run produced more raw rows in Bronze than actual distinct claims existed, traced to the topic retaining messages across producer runs combined with `startingOffsets="earliest"`. This is expected Kafka behavior, not a bug, and is precisely why deduplication is handled explicitly at the Silver boundary rather than assumed away.

**Message keys and partition routing.** Producing with `claim_id` as the message key ensures Kafka's default partitioner routes all messages sharing that key to the same partition consistently — relevant for any downstream processing requiring per-key ordering guarantees.

**Structured Streaming's micro-batch model.** Rather than triggering a full Spark computation per individual message (prohibitively wasteful given Spark's per-batch overhead), a processing-time trigger accumulates messages over a fixed interval and processes them as a small batch — trading a bounded amount of latency for dramatically better throughput, and enabling Structured Streaming's exactly-once processing guarantee via checkpointed offset tracking.

**Checkpointing.** The `checkpointLocation` records exactly which offsets have been successfully committed to the Delta sink; a restarted streaming query resumes precisely from that point, guaranteeing neither reprocessing nor data loss across restarts.

### Terraform: Infrastructure as Code

Every piece of cloud/platform infrastructure in this project prior to Terraform's introduction existed only because of a UI click or an ad hoc SQL statement — meaning there was no single, versioned source of truth describing what the infrastructure should look like. Terraform's `.tf` files serve exactly that role for both the Databricks (schemas, a serverless-compatible job) and Snowflake (warehouse, database, schema, role, grants) surfaces of this project.

**The plan/apply safety model.** Because Terraform configuration is declarative, every `apply` is preceded by a `plan` that computes a diff against real infrastructure state — a discipline that directly caught an import-related issue in this project (Terraform proposing to silently strip system-managed metadata after adopting pre-existing schemas), underscoring why reviewing `plan` output is a non-negotiable habit rather than a formality.

**State and imports.** Terraform's state file is the only thing it trusts to know what it manages — it has no built-in behavior for silently adopting matching, pre-existing infrastructure, by design. Bringing already-existing resources under management requires an explicit `terraform import`, a genuinely common real-world scenario for teams adopting IaC after infrastructure already exists organically.

---

## Performance and Optimization Notes

| Area | Technique Applied | Rationale |
|---|---|---|
| Delta Lake file layout | `OPTIMIZE` + `ZORDER BY (provider_id)` | Compacts small files from frequent writes; co-locates rows for a commonly-filtered column, improving file-skip pruning |
| Spark partitioning | Partition Bronze by `submitted_date` | Moderate cardinality, commonly filtered — avoids over-partitioning while enabling partition pruning |
| Spark shuffle tuning | `spark.sql.shuffle.partitions` set relative to dataset size; `coalesce()` before writes | Prevents the small-file problem at write time rather than requiring after-the-fact compaction |
| Spark join strategy | Explicit `broadcast()` for dimension-table joins | Eliminates a full shuffle of the large fact table for joins against small lookup tables |
| Snowflake compute | `XSMALL` warehouse, `auto_suspend=60`, `auto_resume=true` | Minimizes idle credit consumption against a fixed, non-renewable trial credit balance |
| Snowflake query performance | Verified via Query Profile: partitions scanned vs. total for filtered vs. unfiltered queries | Directly demonstrates micro-partition pruning rather than asserting it abstractly |
| dbt materialization | Views for staging, tables for marts | Balances storage cost/freshness (views) against repeated-query performance (tables) |

---

## Major Problems Encountered

*(See collapsible sections above in the previous version — retained here for real architectural constraints and non-obvious debugging, each with problem, resolution, and why it mattered.)*

<details>
<summary><strong>1. Databricks Free Edition doesn't support classic compute — Terraform scope had to pivot</strong></summary>

**Problem:** the original plan was to provision a Databricks *cluster* as code via the `databricks_cluster` Terraform resource. Free Edition is serverless-only; that resource cannot be used on this workspace tier at all.

**Resolution:** pivoted Terraform scope to what Free Edition actually supports — Unity Catalog schema management (`databricks_schema`) and a job resource defined with no cluster block at all, since omitting the compute specification entirely is what signals serverless execution to Databricks.

**Why it mattered:** demonstrates understanding that "serverless" is the absence of an explicit compute spec, not a separate toggle, and that IaC scope must be matched to what a given platform tier genuinely supports rather than a generic textbook plan.
</details>

<details>
<summary><strong>2. Adopting pre-existing infrastructure into Terraform surfaced real IaC gotchas</strong></summary>

**Problem:** `bronze`, `silver`, and `gold` schemas already existed from earlier manual creation. `terraform apply` attempted to recreate them and failed with "already exists" errors.

**Resolution:** used `terraform import` to bring each schema under management without recreating it. This surfaced a second, less obvious issue: post-import, `terraform plan` proposed removing system-managed properties (`collation`, `owner`, an internal Delta/Iceberg interop flag) that were never declared in the `.tf` configuration — because Terraform treats an omitted map-typed field as "should be empty," not "leave alone."

**Why it mattered:** a genuinely common real-world scenario (teams adopting Terraform after infrastructure already exists organically) with a well-known but easy-to-miss drift behavior, caught only by disciplined review of `plan` output before applying.
</details>

<details>
<summary><strong>3. Free Edition has no direct cloud storage bridge to Snowflake</strong></summary>

**Problem:** a production setup would move Gold-tier data to Snowflake via `COPY INTO ... FROM s3://...` or Snowpipe, reading directly from cloud object storage. Free Edition has no bring-your-own-cloud-storage option.

**Resolution:** built a manual bridge — export via `.toPandas()` to CSV, transfer locally, then `PUT`/`COPY INTO` via SnowSQL using key-pair authentication.

**Why it mattered:** documented explicitly as a Free-Edition-specific workaround rather than the production pattern, with a clear, precise articulation of what the cloud-storage-backed version would look like instead.
</details>

<details>
<summary><strong>4. Fraud-detection rule produced a false positive — investigated to ground truth, not assumed</strong></summary>

**Problem:** the `is_billing_spike` rule flagged 3 providers; only 2 were seeded as fraudulent in the synthetic data generator.

**Resolution:** ground truth was confirmed directly from the underlying claims data (filtering for billed amounts exceeding the maximum possible legitimate procedure cost) rather than attempting to regenerate the original random provider selection, which turned out to be non-reproducible (`uuid.uuid4()` draws from OS entropy, unaffected by `random.seed()`).

**Result:** 2 of 2 true positives, 1 false positive — root-caused to a legitimately expensive procedure landing on an otherwise normal day for that provider, not a logic defect.

**Why it mattered:** produced a specific, defensible precision/recall figure rather than an unverified assumption, and a concrete, credible answer to "how would you improve this."
</details>

<details>
<summary><strong>5. Kafka's at-least-once delivery produced real duplicate data</strong></summary>

**Problem:** following a Structured Streaming run, Bronze held significantly more rows than actual distinct claims existed.

**Root cause:** Kafka topics do not deduplicate by key by default; the topic had retained messages across producer runs, and `startingOffsets="earliest"` meant a fresh consumer read the full backlog.

**Resolution:** required no code change — the Silver-layer deduplication logic, built independently weeks earlier, collapsed Bronze's duplicated rows to the correct distinct claim count automatically.

**Why it mattered:** the single strongest piece of evidence in this project that the medallion architecture is not merely theoretical — it transparently absorbed a genuine, unplanned data quality event.
</details>

<details>
<summary><strong>6. Local Kafka is unreachable from remote Databricks compute</strong></summary>

**Problem:** a Structured Streaming job referencing a local broker address failed to connect when executed as Databricks compute — the address resolves to the remote executor itself, not the local development machine, and is further constrained by Free Edition's restricted outbound network access.

**Resolution:** validated the full producer-to-consumer streaming path locally against a local Delta Lake path, then batch-appended the verified result into the actual Unity Catalog table via the existing authenticated Databricks connection.

**Why it mattered:** surfaced a fundamental distributed-systems distinction — authentication to manage a system (control plane) is entirely separate from that system's workload being network-reachable to an arbitrary external resource (data plane).
</details>

---

## Fraud Detection Methodology and Results

**Approach.** A rolling-baseline anomaly detection rule was implemented entirely in dbt SQL: for each provider, a 7-day trailing average of daily billed amounts is computed via a window function, and any day whose average exceeds 3x that trailing baseline is flagged (`is_billing_spike`). This intentionally simple, interpretable rule was chosen as a first-pass signal — the kind of rule a real fraud analytics team would implement before investing in a full ML model, and a natural baseline against which a future ML-based approach could be benchmarked.

**Validation methodology.** Rather than accept the model's output at face value, the actual seeded ground truth (2 providers, each with a 4-6x billing multiplier applied to ~15% of their claims) was cross-referenced directly against flagged results using the underlying raw claims data — not by attempting to regenerate the original random selection, which was confirmed to be non-reproducible due to the interaction between Python's `uuid4()` (OS-entropy-based) and its seeded `random` module.

**Results:**

| Metric | Result |
|---|---|
| True positives | 2 of 2 seeded fraud providers |
| False positives | 1 |
| Recall | 100% |
| Precision | 67% |

**Root cause of the false positive:** a legitimately expensive procedure (a CT scan, near the top of its normal price range) landing on an otherwise typical day for that provider was sufficient to push the day's average past the 3x threshold — demonstrating a real limitation of a rule that reasons purely on aggregate dollar totals without accounting for procedure-mix composition. This is documented as a specific, understood shortcoming with a concrete proposed remediation (conditioning the threshold on procedure mix, or requiring persistence across multiple days before flagging) — a substantially stronger result to present than an unrealistically perfect, unexamined one.

---

## Engineering Retrospective

- **Layered validation as a design principle, not just a debugging habit.** Nearly every issue in this project was caught by checking one specific, narrow thing — table existence, row counts, distinct-key counts, ground-truth cross-references — before assuming a broader system was working correctly. This mirrors, informally, exactly what dbt's automated test suite formalizes: `not_null`, `unique`, `relationships`, and range checks are the codified version of the same manual verification discipline applied throughout this build.

- **Medallion architecture as a real operational boundary, not a naming convention.** The clearest evidence of this: a genuine Kafka at-least-once delivery event produced real duplicate data in Bronze, and it was absorbed completely by Silver's deduplication logic with zero manual intervention — because that logic was built to handle exactly this class of problem, well before Kafka was even introduced into the project.

- **Platform constraints as architectural inputs, not obstacles to route around silently.** Databricks Free Edition's serverless-only model and lack of direct cloud storage access, and Snowflake's non-renewable trial window, each shaped specific, documented design decisions (Terraform scope, the batch-export bridge to Snowflake, the Week 11 sequencing) rather than being treated as inconveniences to paper over.

- **Infrastructure-as-code discipline extends beyond initial provisioning.** The `plan`-before-`apply` habit, and the specific behavior of Terraform state around imported resources with undeclared fields, are lessons that only surface once existing infrastructure needs to be brought under management — a common real-world scenario that a greenfield-only Terraform exercise would never have surfaced.

- **An honestly-reported imperfect result carries more credibility than an unexamined perfect one.** A fraud-detection rule with 100% recall and zero false positives, reported without scrutiny, would understate the amount of validation actually performed. The documented 67% precision, together with its specific, verified root cause, is direct evidence of methodological rigor rather than a limitation to minimize.

---

## Future Enhancements

- **Procedure-mix-aware fraud scoring** — condition the spike threshold on a provider's typical procedure distribution rather than raw aggregate billing, directly addressing the documented false positive's root cause.
- **Multi-day persistence requirement** — require an anomaly to hold across consecutive days before flagging, reducing sensitivity to single-day variance.
- **ML-based anomaly detection benchmark** — introduce a scikit-learn isolation forest or similar unsupervised model on the same feature set, benchmarked directly against the current rule-based baseline's precision/recall.
- **Cloud-storage-backed Snowflake load** — on a non-Free-Edition Databricks workspace, replace the manual CSV export/`PUT` bridge with a direct `COPY INTO ... FROM s3://...` or Snowpipe-based load, removing the manual handoff step entirely.
- **Remote Terraform state** — migrate from local state to a remote backend (S3 + DynamoDB locking, or Terraform Cloud) to reflect a team-scale, multi-contributor setup.
- **CI-triggered dbt tests** — run `dbt test` on every pull request via GitHub Actions, rather than only as an Airflow-scheduled task.

---

## Getting Started

**Prerequisites:** Docker Desktop (with WSL2 backend on Windows), Python 3.11+, Terraform CLI, a Databricks Free Edition workspace, and (optionally, for the serving layer) a Snowflake trial account.

```bash
# 1. Clone and start local infrastructure (Airflow + Redpanda)
git clone <https://github.com/sathwikhnaik/claims-lakehouse-platform>
cd claims-lakehouse-platform
docker compose up -d --build

# 2. Generate synthetic claims data
python3 data_generator/generate_claims.py

# 3. Provision Databricks schemas + job
cd terraform/databricks && terraform init && terraform apply

# 4. Run the lakehouse dbt project
cd ../../dbt/lakehouse_transform && dbt run && dbt test

# 5. (Optional, time-boxed) Provision Snowflake and run the serving_marts project
cd ../../terraform/snowflake && terraform init && terraform apply
cd ../../dbt/serving_marts && dbt run && dbt test

# 6. Trigger the full pipeline via Airflow
# open http://localhost:8080, unpause and trigger `full_pipeline`
```

See `dashboard/build_dashboard.py` for generating a standalone dashboard export at any time.

---

## Snowflake Lifecycle Note

The Snowflake serving layer was provisioned and demonstrated during a time-boxed, non-renewable trial window and is not expected to remain live indefinitely. Durable proof of its output — including the generated dashboard, query profile screenshots demonstrating pruning behavior, and exported sample data — is preserved in `dashboard/` and `sample_data/`. The Databricks Free Edition / Delta Lake lakehouse layer has no expiry and remains fully live and reproducible independent of the Snowflake trial's lifecycle.