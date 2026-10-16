# ELT Transformations

## 1. Overview

The Meta Data Warehouse uses a scheduled batch ELT job to transform raw Trino query events into an analytical data model in Snowflake.

The ingestion process is responsible for loading source data into the RAW layer. The ELT job then transforms this data into dimensions, fact tables, and bridge tables.

The job is scheduled to run periodically, preferably during periods of lower expected resource utilization. A daily schedule is a reasonable starting point, although the actual frequency should depend on data freshness requirements and available compute resources.

The transformation pipeline consists of the following steps:

1. Populate `DIM_DATE` with dates required by the analytical model.
2. Populate `DIM_USER` from `RAW_USERS`.
3. Populate `DIM_TEAM` from `RAW_TEAMS`.
4. Extract table references from query text and use them to populate `DIM_TABLE`.
5. Populate `FACT_QUERY` with query execution data and resource consumption metrics.
6. Populate `BRIDGE_QUERY_TABLE` to associate query executions with the tables they accessed.

The transformations are incremental: existing records are reused, and new records are inserted when new source data becomes available.

## 2. Source Tables and Assumptions

The transformations use the following source tables:

| Source table | Purpose |
|---|---|
| `RAW_QUERY_EVENTS` | Query execution events and resource consumption metrics. |
| `RAW_USERS` | User metadata. |
| `RAW_TEAMS` | Team metadata. |

The implementation assumes that `RAW_QUERY_EVENTS` contains the following columns:

- `query_id`
- `user_id`
- `team_id`
- `event_timestamp`
- `query_text`
- `cpu_time_ms`
- `execution_duration_ms`
- `processed_bytes`
- `processed_rows`

The actual source schema must be verified before implementation.

### Table Reference Extraction

The source events contain `query_text`, but do not directly provide the structured list of tables accessed by each query.

Table references are therefore extracted from the query text as part of the transformation pipeline.

The extraction produces an intermediate relation named `STAGED_QUERY_TABLE_REFERENCES`, containing:

| Column | Description |
|---|---|
| `query_id` | Identifier of the query execution. |
| `catalog_name` | Catalog containing the referenced table. |
| `schema_name` | Schema containing the referenced table. |
| `table_name` | Name of the referenced table. |

The implementation of SQL parsing is outside the scope of this document. The transformations described below assume that this intermediate data is available.

This part could be done after extracting data from Iceberg to Snowflake staging tables. The choice to do it now is so we could store actual raw data in raw tables.

## 3. Transformation Strategy

The pipeline follows a defined execution order:

1. Populate dimensions.
2. Populate the query fact table.
3. Populate the query-table bridge table.

Dimensions are populated before the fact and bridge tables so that the corresponding dimension records are available when relationships are created.

The transformations use `MERGE` statements where appropriate to avoid inserting records that already exist.

For table identification, the combination of `catalog_name`, `schema_name`, and `table_name` identifies a table in the current model. A matching record in `DIM_TABLE` is reused; a new `table_id` is generated only for a previously unseen combination.

The design assumes that one scheduled job instance performs the transformations at a time. Concurrent executions must be prevented or otherwise coordinated to avoid duplicate inserts.

## 4. Populating Dimensions

### 4.1. DIM_DATE

`DIM_DATE` contains one row per calendar date.

The initial implementation inserts dates associated with query events. Additional calendar dates can be generated independently if a continuous calendar is required.

```sql
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_id NUMBER(8, 0) PRIMARY KEY,
    full_date DATE NOT NULL
);
```

The following statement inserts any dates present in the source events that are not already in the dimension:

```sql
MERGE INTO DIM_DATE AS target
USING (
    SELECT DISTINCT
        TO_NUMBER(TO_CHAR(event_timestamp::DATE, 'YYYYMMDD')) AS date_id,
        event_timestamp::DATE AS full_date
    FROM RAW_QUERY_EVENTS
    WHERE event_timestamp IS NOT NULL
) AS source
ON target.date_id = source.date_id

WHEN NOT MATCHED THEN
    INSERT (
        date_id,
        full_date
    )
    VALUES (
        source.date_id,
        source.full_date
    );
```

The `date_id` uses the `YYYYMMDD` representation of the calendar date. So the actual date is the key in a table which is quite handy when checking if a date already exists in table.

### 4.2. DIM_USER

`DIM_USER` stores descriptive user information.

```sql
CREATE TABLE IF NOT EXISTS DIM_USER (
    user_id VARCHAR PRIMARY KEY,
    user_name VARCHAR
);
```

The dimension is populated from `RAW_USERS`.

```sql
MERGE INTO DIM_USER AS target
USING (
    SELECT
        user_id,
        user_name
    FROM RAW_USERS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY user_id
        ORDER BY user_id
    ) = 1
) AS source
ON target.user_id = source.user_id

WHEN MATCHED THEN
    UPDATE SET
        user_name = source.user_name

WHEN NOT MATCHED THEN
    INSERT (
        user_id,
        user_name
    )
    VALUES (
        source.user_id,
        source.user_name
    );
```

This transformation inserts new users and updates the stored name when an existing user's name changes.

### 4.3. DIM_TEAM

`DIM_TEAM` stores descriptive team information.

```sql
CREATE TABLE IF NOT EXISTS DIM_TEAM (
    team_id VARCHAR PRIMARY KEY,
    team_name VARCHAR
);
```

The dimension is populated from `RAW_TEAMS`.

```sql
MERGE INTO DIM_TEAM AS target
USING (
    SELECT
        team_id,
        team_name
    FROM RAW_TEAMS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY team_id
        ORDER BY team_id
    ) = 1
) AS source
ON target.team_id = source.team_id

WHEN MATCHED THEN
    UPDATE SET
        team_name = source.team_name

WHEN NOT MATCHED THEN
    INSERT (
        team_id,
        team_name
    )
    VALUES (
        source.team_id,
        source.team_name
    );
```

This transformation inserts new teams and updates their descriptive information when necessary.

The model does not maintain a history of team names. Historical query attribution is instead preserved by storing the relevant `team_id` in `FACT_QUERY`.

As with `DIM_USER`, the source is expected to contain a single logical record per team.

### 4.4. DIM_TABLE

`DIM_TABLE` stores metadata about tables referenced by Trino queries.

```sql
CREATE TABLE IF NOT EXISTS DIM_TABLE (
    table_id NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    catalog_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL
);
```

The combination of `catalog_name`, `schema_name`, and `table_name` identifies a table in the current model.

The transformation first deduplicates the extracted table references and then inserts only combinations that do not already exist.

```sql
MERGE INTO DIM_TABLE AS target
USING (
    SELECT DISTINCT
        catalog_name,
        schema_name,
        table_name
    FROM STAGED_QUERY_TABLE_REFERENCES
    WHERE catalog_name IS NOT NULL
      AND schema_name IS NOT NULL
      AND table_name IS NOT NULL
) AS source
ON target.catalog_name = source.catalog_name
   AND target.schema_name = source.schema_name
   AND target.table_name = source.table_name

WHEN NOT MATCHED THEN
    INSERT (
        catalog_name,
        schema_name,
        table_name
    )
    VALUES (
        source.catalog_name,
        source.schema_name,
        source.table_name
    );
```

Snowflake generates `table_id` for each newly inserted dimension record.

When a query references a table that already exists in `DIM_TABLE`, the existing record is reused.

This avoids repeatedly storing the same table metadata and provides a stable surrogate key within the DWH.

The implementation assumes that table names are normalized consistently and that the three-part name is sufficient to identify a table for the required analyses.

## 5. Populating FACT_QUERY

`FACT_QUERY` stores one record per completed query execution.

```sql
CREATE TABLE IF NOT EXISTS FACT_QUERY (
    query_id VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    team_id VARCHAR NOT NULL,
    date_id NUMBER(8, 0) NOT NULL,
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    cpu_time_ms NUMBER,
    execution_duration_ms NUMBER,
    processed_bytes NUMBER,
    processed_rows NUMBER
);
```

The transformation selects completed query events, derives `date_id` from `event_timestamp`, and inserts query executions that do not already exist.

```sql
MERGE INTO FACT_QUERY AS target
USING (
    SELECT
        query_id,
        user_id,
        team_id,
        TO_NUMBER(
            TO_CHAR(event_timestamp::DATE, 'YYYYMMDD')
        ) AS date_id,
        event_timestamp,
        cpu_time_ms,
        execution_duration_ms,
        processed_bytes,
        processed_rows
    FROM RAW_QUERY_EVENTS
    WHERE query_id IS NOT NULL
      AND user_id IS NOT NULL
      AND team_id IS NOT NULL
      AND event_timestamp IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY query_id
        ORDER BY event_timestamp DESC
    ) = 1
) AS source
ON target.query_id = source.query_id

WHEN NOT MATCHED THEN
    INSERT (
        query_id,
        user_id,
        team_id,
        date_id,
        event_timestamp,
        cpu_time_ms,
        execution_duration_ms,
        processed_bytes,
        processed_rows
    )
    VALUES (
        source.query_id,
        source.user_id,
        source.team_id,
        source.date_id,
        source.event_timestamp,
        source.cpu_time_ms,
        source.execution_duration_ms,
        source.processed_bytes,
        source.processed_rows
    );
```

The transformation deduplicates the source batch by `query_id` and inserts only query executions that do not already exist in `FACT_QUERY`.

Because the source is assumed to provide final records for completed queries, existing fact records are not updated.

The transformation excludes events missing required identifiers or timestamps. In a production implementation, invalid records can be written to a separate quarantine table together with a rejection reason, allowing them to be investigated and reprocessed once the missing or invalid data is corrected. Monitoring and alerting can track the number of rejected records and notify the responsible team when predefined thresholds are exceeded.

The dimension records referenced by `user_id`, `team_id`, and `date_id` should be validated as part of the pipeline. Snowflake standard-table primary key constraints are generally not enforced, so the transformation logic must maintain key integrity.

## 6. Populating BRIDGE_QUERY_TABLE

`BRIDGE_QUERY_TABLE` represents the many-to-many relationship between query executions and tables.

```sql
CREATE TABLE IF NOT EXISTS BRIDGE_QUERY_TABLE (
    query_id VARCHAR NOT NULL,
    table_id NUMBER NOT NULL,
    PRIMARY KEY (query_id, table_id)
);
```

The bridge table contains only the identifiers of the query execution and the accessed table.

The transformation joins the extracted table references with `FACT_QUERY` and `DIM_TABLE`, then inserts any query-table relationships that do not already exist.

```sql
MERGE INTO BRIDGE_QUERY_TABLE AS target
USING (
    SELECT DISTINCT
        r.query_id,
        t.table_id
    FROM STAGED_QUERY_TABLE_REFERENCES AS r
    JOIN FACT_QUERY AS q
        ON q.query_id = r.query_id
    JOIN DIM_TABLE AS t
        ON t.catalog_name = r.catalog_name
       AND t.schema_name = r.schema_name
       AND t.table_name = r.table_name
) AS source
ON target.query_id = source.query_id
   AND target.table_id = source.table_id

WHEN NOT MATCHED THEN
    INSERT (
        query_id,
        table_id
    )
    VALUES (
        source.query_id,
        source.table_id
    );
```

The join to `FACT_QUERY` ensures that the bridge table only contains relationships for query executions present in the analytical model.

The join to `DIM_TABLE` resolves each table reference to its corresponding surrogate key.

The `DISTINCT` operation removes duplicate query-table pairs within the current source batch.

The transformation assumes that `STAGED_QUERY_TABLE_REFERENCES` contains the references extracted for the query events being processed.

## 7. Incremental Processing and Idempotency

The batch job should avoid repeatedly transforming the entire source history as the dataset grows. Two possible approaches are considered.

**Watermark-based processing:** The job tracks a reliable source watermark, such as an ingestion timestamp or another source-supported incremental processing cursor. Each run processes records newer than the last successfully processed position. If timestamp-based processing is used, a small overlapping window can reduce the risk of missing late-arriving events. Deduplication and merge logic prevent previously processed records from being inserted again.

**Date-range processing:** Alternatively, each run can rebuild a recent date range, such as the previous few days. The job deletes the existing records within that range and inserts the corresponding records from the raw tables again. This approach naturally incorporates late-arriving events and corrections within the selected range, provided the source data is available. The delete and insert operations must be executed atomically to avoid leaving the target tables partially populated if a run fails. Related tables, including `BRIDGE_QUERY_TABLE`, must be refreshed consistently.

Regardless of the extraction strategy, the pipeline should follow these principles:

- Dimension records are reused when their natural keys already exist.
- New query executions are inserted into `FACT_QUERY`.
- Existing query executions are not updated because source records are assumed to be final.
- New query-table relationships are inserted into `BRIDGE_QUERY_TABLE`.
- Reprocessing the same input does not create duplicate logical records.
- Watermarks, if used, are advanced only after the corresponding processing succeeds.
- Standard Snowflake table constraints are not sufficient protection against duplicate records; uniqueness must be maintained by the transformation logic.
- The scheduled job must avoid overlapping executions.

The current insert-only approach assumes that completed source records are immutable. If existing records can be corrected, the pipeline must either update matched records or rebuild the affected date range. A date-range refresh can capture corrections only within the range being reprocessed.

## 8. Python Orchestration - Scheduling a Batch Job

The ELT transformations can be orchestrated using Apache Airflow, which allows the pipeline to run on a schedule and defines dependencies between individual tasks.

For example, the pipeline could validate the availability of source data, populate the dimension tables, load query-level facts, and finally populate the bridge table. This ordering ensures that the referenced dimensions and fact records exist before the bridge relationships are created.

The following example illustrates how such a pipeline could be defined using an Airflow DAG. The task execution functions are placeholders for the corresponding Snowflake SQL transformations described above.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "mixi-o",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="trino_usage_elt",
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=datetime(2026, 10, 15),
    catchup=False,
    max_active_runs=1,
) as dag:

    validate_source = PythonOperator(
        task_id="validate_source_data",
        python_callable=validate_source_data,
    )

    populate_dimensions = PythonOperator(
        task_id="populate_dimensions",
        python_callable=populate_dimensions,
    )

    populate_fact = PythonOperator(
        task_id="populate_fact_query",
        python_callable=populate_fact_query,
    )

    populate_bridge = PythonOperator(
        task_id="populate_bridge_query_table",
        python_callable=populate_bridge_query_table,
    )

    (
        validate_source
        >> populate_dimensions
        >> populate_fact
        >> populate_bridge
    )
```

The callable functions represent operations that would execute the corresponding validation and SQL statements against Snowflake. In a complete implementation, they would establish connections using appropriately configured credentials and execute the transformations.

Airflow can schedule the pipeline, retry failed tasks, and prevent overlapping DAG runs. The transformations themselves remain in SQL, while Python handles orchestration.