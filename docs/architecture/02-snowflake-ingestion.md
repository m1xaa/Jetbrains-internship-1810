# Snowflake Ingestion

## 1. Overview

The purpose of the ingestion layer is to make the existing Trino query events available in Snowflake so that they can be transformed into the analytical data model.

Our responsibility is to establish the ingestion process between the existing source data(Apache Iceberg Tables) and Snowflake.

The ingestion layer should:

- Make the source events available in Snowflake.
- Support incremental processing where appropriate.
- Avoid inserting duplicate events.
- Handle events that arrive late.
- Keep the raw data available for downstream transformations.

The exact implementation depends on how the existing Iceberg tables are exposed to Snowflake. There are two main approaches: querying the Iceberg tables directly or copying their data into Snowflake-managed tables.

## 2. Ingestion Approaches

### 2.1. Direct Access to Iceberg Tables

Snowflake supports Apache Iceberg tables through its Iceberg table integration. Depending on the existing configuration, Snowflake can access Iceberg table metadata and the underlying data files stored in S3.

In this setup, we do not necessarily need to copy all the source data into a separate Snowflake raw table. Instead, we can query the source Iceberg data from Snowflake and use it as the input to our transformations.

The main advantage is that we avoid unnecessary data duplication.

However, direct access requires the appropriate Snowflake integration, catalog configuration, and permissions to access the underlying data.

The actual SQL used to create or access an Iceberg table depends on the catalog and metadata configuration. Therefore, this project should reuse the existing configuration where possible rather than assume that a new Iceberg integration must be created.

### 2.2. Loading Data into Snowflake Tables

Alternatively, we can copy the source data into Snowflake-managed tables.

This approach gives us control over the raw data stored in Snowflake and allows us to manage ingestion independently of downstream transformations.

For this approach, the ingestion process consists of:

1. Accessing the source data or exporting it to a format Snowflake can read.
2. Registering the source files through a Snowflake stage.
3. Loading the data into a staging table.
4. Deduplicating the staged events.
5. Merging the events into the raw Snowflake table.

The following sections illustrate this approach using Parquet files in S3. This is an example of a possible implementation, not an assumption that the existing Iceberg tables can be ingested by reading their Parquet files directly.

This is the approach that we chose since it gives us complete control over data since it is placed in our meta DWH.

## 3. Accessing Source Files

An Iceberg table is more than a collection of Parquet files. Iceberg metadata identifies the files belonging to a table and tracks snapshots, schema changes, and other table information, so we need to take that metadata into account.

If the source data is made available as Parquet files for ingestion, Snowflake can access those files through an external stage.

The following example assumes that the relevant files have been exported or otherwise made available as a consistent Parquet dataset.

### 3.1. File Format

We define a reusable file format for reading Parquet files:

```sql
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = PARQUET;
```

### 3.2. External Stage

An external stage provides a named reference to files stored in S3.

The following is an illustrative example:

```sql
CREATE OR REPLACE STAGE raw_events_stage
    URL = 's3://example-bucket/trino-events/'
    STORAGE_INTEGRATION = s3_storage_integration
    FILE_FORMAT = parquet_format;
```

Here, `s3_storage_integration` represents a Snowflake storage integration that must be configured with the appropriate AWS permissions.

The bucket URL, integration name, and path must be replaced with the actual environment configuration.

The stage makes the files accessible to Snowflake.

## 4. Staging Raw Events

Before merging events into the target table, we can load them into a staging table.

For illustration, assume that each event contains the following fields:


- `query_id` - identifier of a query execution.
- `event_timestamp` - timestamp associated with the source event.
- `user_id` - identifier of the user who executed the query.
- `team_id` - identifier of a team that a user is a part of
- `query_text` - SQL text submitted for execution.
- `execution_duration_ms` - elapsed execution time, where available.
- `cpu_time_ms` - CPU time consumed by the query, where available.
- `processed_bytes` - amount of input data processed, where available.
- `processed_rows` - number of processed rows, where available.

These are example fields. The actual staging schema must be based on the source event structure.

We can define the staging table as follows:

```sql
CREATE OR REPLACE TABLE STAGED_QUERY_EVENTS (
    query_id VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    user_id VARCHAR,
    team_id VARCHAR,
    query_text VARCHAR,
    cpu_time_ms NUMBER,
    execution_duration_ms NUMBER,
    processed_bytes NUMBER,
    processed_rows NUMBER
);
```

If the data is available as Parquet files, Snowflake can load it using `COPY INTO`:

```sql
COPY INTO STAGED_QUERY_EVENTS
FROM @raw_events_stage
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
FILE_FORMAT = (TYPE = PARQUET);
```

This example assumes that the Parquet column names and types are compatible with the staging table.

In a production implementation, we should also define how files are selected, how ingestion progress is tracked, and whether staging data is replaced or appended for each run.

The staging table should represent a clearly defined ingestion batch so that the same batch can be processed safely if a previous run fails.

## 5. Incremental Ingestion

Reloading all historical events on every run would be unnecessarily expensive. Instead, we want to process new events while allowing for events that arrive late.

The exact incremental strategy depends on how the source data is exposed.

### 5.1. Watermark-Based Processing

If the source provides a reliable event timestamp, we can track the latest successfully processed timestamp in a control table.

For example:

```sql
CREATE TABLE IF NOT EXISTS INGESTION_WATERMARK (
    source_name VARCHAR,
    last_processed_timestamp TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ
);
```

The watermark records the processing boundary for a source.

For an initial implementation, we can reprocess a short period before the last recorded watermark. This overlapping window helps capture events that arrive late.

For example, if the last processed timestamp is `2026-10-14 12:00:00`, the next batch might include events from `2026-10-14 11:45:00` onward.

Conceptually, the source selection would look like this:

```sql
SELECT *
FROM SOURCE_QUERY_EVENTS
WHERE event_timestamp >= :window_start;
```

Here, `:window_start` is calculated from the stored watermark minus the configured overlap interval.

`SOURCE_QUERY_EVENTS` is a placeholder for the actual source table or query. This statement illustrates the filtering logic; it is not a complete ingestion implementation.

After the batch has been processed successfully, the watermark can be advanced to the new processing boundary.

The watermark should only advance after the relevant batch has been processed successfully. Otherwise, a failed run could cause events to be skipped.

### 5.2. Limitations of Timestamp-Based Ingestion

Timestamp-based ingestion depends on the availability and reliability of the source event timestamp.

Events may arrive late, so the ingestion process can reprocess a short period before the last successfully processed watermark. This overlapping
window reduces the risk of missing late events, while deduplication prevents previously ingested query executions from being inserted again.

The overlap does not guarantee that every late event will be captured. If the source supports a reliable incremental mechanism based on Iceberg
snapshots or other change metadata, that mechanism may be preferable.

## 6. Deduplication and Loading into Raw Tables

Since the source is assumed to provide one final record for each completed query execution, existing records do not need to be updated after ingestion.

However, duplicate records may appear in a staging batch when ingestion reprocesses an overlapping time window or retries a previous batch.

We first remove duplicates within the current batch:

```sql
SELECT *
FROM STAGED_QUERY_EVENTS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY query_id
    ORDER BY event_timestamp DESC
) = 1;
```

This keeps one record per `query_id`. The query assumes that `query_id` uniquely identifies a query execution. The actual key must be verified against the source schema.

After deduplication, we insert only records that do not already exist in `RAW_QUERY_EVENTS`:

```sql
INSERT INTO RAW_QUERY_EVENTS (
    query_id,
    event_timestamp,
    user_id,
    team_id,
    query_text,
    execution_duration_ms,
    cpu_time_ms,
    processed_bytes,
    processed_rows
)
SELECT
    s.query_id,
    s.event_timestamp,
    s.user_id,
    s.team_id,
    s.query_text,
    s.execution_duration_ms,
    s.cpu_time_ms,
    s.processed_bytes,
    s.processed_rows
FROM (
    SELECT *
    FROM STAGED_QUERY_EVENTS
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY query_id
        ORDER BY event_timestamp DESC
    ) = 1
) AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM RAW_QUERY_EVENTS AS r
    WHERE r.query_id = s.query_id
);
```

## 7. User and Team Metadata

User and team metadata may be available from separate source table, such as `STAGED_USERS`, `RAW_USERS` and `STAGED_TEAMS`, `RAW_TEAMS`. These tables could contain following fields and some additional information if accessible:

- `user_id` - identifier of the user who executed the query.
- `user_name` - username of a user.

- `team_id` - identifier of a team that a user was a part of when this query was executed.
- `team_name` - name of a team.

These datasets are joined during downstream transformations to associate each query execution with the relevant user and team.

## 8. Handling Failed Runs

The ingestion process should be safe to retry.

A batch should not be marked as successfully processed until its data has been loaded and merged successfully. If a run fails, the same batch should be eligible for processing again.

This can be implemented by recording batch status and watermark progress in a control table.

For example, a production implementation could track:

- Source name.
- Batch identifier.
- Batch start and end boundaries.
- Processing status.
- Number of records read.
- Number of records inserted or updated.
- Processing start and completion timestamps.

The ingestion process should also validate the batch before advancing its watermark.