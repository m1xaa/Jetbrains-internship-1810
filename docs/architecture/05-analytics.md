# Analytics Queries

Here are written queries that answer the quesitons form the task's description. 

## 1. Distinct Users Querying a Table During a Period

Count the distinct users who queried a table during a given period

```sql

SELECT 
    COUNT(DISTINCT fq.user_id) as distinct_users
FROM fact_query fq
JOIN bridge_query_table bqt
    ON fq.query_id = bqt.query_id
JOIN dim_table dt 
    ON bqt.table_id = dt.table_id
WHERE dt.catalog_name = 'iceberg'
    AND dt.schema_name = 'analytics'
    AND dt.table_name = 'orders'
    AND fq.event_timestamp >= '2026-10-01'
    AND fq.event_timestamp < '2026-11-01';

```

## 2. Resource Consumption by a Team During a Period

Calculate the total resources consumed by a team during a given period. The metrics include CPU time, query execution duration, processed bytes and processed rows.

```sql

SELECT
    t.team_name,
    SUM(fq.cpu_time_ms) AS total_cpu_time_ms,
    SUM(fq.execution_duration_ms) AS total_execution_duration_ms,
    SUM(fq.processed_bytes) AS total_processed_bytes,
    SUM(fq.processed_rows) AS total_processed_rows
FROM FACT_QUERY fq
JOIN DIM_TEAM t
    ON fq.team_id = t.team_id
WHERE t.team_name = 'Team Y'
  AND fq.event_timestamp >= '2026-10-01'
  AND fq.event_timestamp < '2026-11-01'
GROUP BY t.team_name;

```

## Important

All string values in queries are random and are placed only as a placeholder.