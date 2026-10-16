# Meta DWH Data Model

## 1. Overview

The Meta Data Warehouse (DWH) is designed to analyze how Trino query executions use data tables and consume computational resources over time.

The model consists of one fact table, one bridge table, and four dimensions:

- `FACT_QUERY` — stores information about query executions and their resource consumption.
- `BRIDGE_QUERY_TABLE` — connects query executions to the tables they accessed.
- `DIM_DATE` — provides a date dimension for time-based analysis.
- `DIM_USER` — stores user information.
- `DIM_TEAM` — stores team information.
- `DIM_TABLE` — stores metadata about tables accessed by queries.

The model separates query execution metrics from table access relationships. This allows us to analyze resource consumption without double-counting queries that access multiple tables.

## 2. Table Granularity and Structure

### 2.1. FACT_QUERY

**Granularity:** One row per completed query execution.

This is the central fact table of the model. It stores resource consumption metrics and identifies the user, team, and date associated with each execution.

| Column | Description |
|---|---|
| `query_id` | Unique identifier of a query execution. Primary key. |
| `user_id` | Identifier of the user who executed the query. |
| `team_id` | Identifier of the team the user belonged to when the query was executed. |
| `date_id` | Foreign key referencing `DIM_DATE`. |
| `event_timestamp` | Timestamp associated with the query execution. |
| `cpu_time_ms` | CPU time consumed by the query, in milliseconds. |
| `execution_duration_ms` | Total query execution duration, in milliseconds. |
| `processed_bytes` | Number of input bytes processed by the query. |
| `processed_rows` | Number of input rows processed by the query. |

The resource metrics are examples of measures that can be used for analysis. Their availability and exact semantics must be verified against the source events.

Both `date_id` and `event_timestamp` are stored because they serve different purposes. `date_id` supports joins with the date dimension, while `event_timestamp` preserves the exact timestamp and allows filtering by arbitrary time ranges.

The `team_id` represents the user's team at the time of execution, rather than necessarily their current team. This ensures that historical query executions remain associated with the appropriate team when membership changes, provided historical membership information is available.

### 2.2. DIM_DATE

**Granularity:** One row per calendar date.

This dimension supports filtering and grouping query executions by date.

| Column | Description |
|---|---|
| `date_id` | Unique identifier of a date, for example `20261014`. Primary key. |
| `full_date` | The calendar date. |

Additional attributes, such as the month or day of the week, can be derived from `full_date` when needed. They are not stored initially because they are not required for the current use cases.

### 2.3. DIM_USER

**Granularity:** One row per user.

This dimension stores descriptive information about users.

| Column | Description |
|---|---|
| `user_id` | Unique identifier of a user. Primary key. |
| `user_name` | User's name, if available in the source metadata. |

The exact attributes depend on the user metadata available from the source systems.

### 2.4. DIM_TEAM

**Granularity:** One row per team.

This dimension stores descriptive information about teams.

| Column | Description |
|---|---|
| `team_id` | Unique identifier of a team. Primary key. |
| `team_name` | Name of the team. |

Team information is stored separately from user information to avoid repeating team names for every user or query execution.

Each record in `FACT_QUERY` also contains a `team_id`. This preserves the team attribution of an individual query execution, even if the user later changes teams.

### 2.5. DIM_TABLE

**Granularity:** One row per distinct table, identified by its catalog, schema, and table name.

This dimension stores metadata about tables accessed by Trino queries. Instead of repeating the table names in every query-table relationship, the model assigns each distinct table a surrogate key, `table_id`.

| Column | Description |
|---|---|
| `table_id` | Unique identifier of a table within the DWH. Primary key. |
| `catalog_name` | Name of the catalog containing the table. |
| `schema_name` | Name of the schema containing the table. |
| `table_name` | Name of the table. |

The combination of `catalog_name`, `schema_name`, and `table_name` identifies a table in the current model.

When new query events are processed, the transformation checks whether the corresponding table already exists in `DIM_TABLE`. If it does, the existing `table_id` is reused. Otherwise, a new dimension record is inserted and assigned a new `table_id`.

This prevents the same table from being inserted repeatedly as new queries access it.

Name comparison and normalization must be handled consistently. The implementation should respect Trino identifier semantics and avoid merging distinct tables because of inappropriate case normalization.

The model assumes that the three-part name is sufficient to identify a table for the required analyses. If tables can be renamed or moved and their identity must be preserved across such changes, a stable identifier from Trino or catalog metadata could be introduced in a future improvement.

### 2.6. BRIDGE_QUERY_TABLE

**Granularity:** One row per query execution and accessed table.

This bridge table represents the many-to-many relationship between query executions and tables.

| Column | Description |
|---|---|
| `query_id` | Identifier of the query execution. Foreign key referencing `FACT_QUERY`. |
| `table_id` | Identifier of the accessed table. Foreign key referencing `DIM_TABLE`. |

A query may access multiple tables, and a table may be accessed by many queries. Therefore, a single `query_id` can appear in multiple rows, and a single `table_id` can be associated with multiple query executions.

The combination of `query_id` and `table_id` serves as the logical composite key, assuming each accessed table is recorded only once per query execution.

Keeping only the identifiers in this table avoids repeatedly storing catalog, schema, and table names. The descriptive table metadata is retrieved through `DIM_TABLE`.

### 2.7 Alternative Approach for storing tables

Another approach would be to keep records in this format `query_id`, `catalog_name`, `schema_name`, and `table_name` instead of keeping `BRIDGE_QUERY_TABLE` and `DIM_TABLE`. Then we could extract tables from each query and just append it to the table called `QUERY_TABLE_DIM` for example. The upside of this approach would be that we would need one less join per query, but the major downside is that for each query we would need to insert new rows into this `QUERY_TABLE_DIM`, one row for each accessed table in query (a lot of entries). That is why I decided to not move forward with this approach. And also, number of tables usually is not that large, so the current `DIM_TABLE` table shouldn't contain a lot of rows, hence making the join on this table not that expensive.

## 3. Relationships

The tables are connected through the following relationships:

```text
                         DIM_DATE
                            |
                         date_id
                            |
DIM_USER --- user_id --- FACT_QUERY --- team_id --- DIM_TEAM
                            |
                         query_id
                            |
                  BRIDGE_QUERY_TABLE
                            |
                         table_id
                            |
                        DIM_TABLE
```

The relationships are:

- `FACT_QUERY.date_id` references `DIM_DATE.date_id`.
- `FACT_QUERY.user_id` references `DIM_USER.user_id`.
- `FACT_QUERY.team_id` references `DIM_TEAM.team_id`.
- `BRIDGE_QUERY_TABLE.query_id` references `FACT_QUERY.query_id`.
- `BRIDGE_QUERY_TABLE.table_id` references `DIM_TABLE.table_id`.

Each query execution is associated with one user, one team at execution time, and one date. It may be associated with multiple accessed tables.

Each distinct table is represented once in `DIM_TABLE` and can be linked to many query executions through `BRIDGE_QUERY_TABLE`.

## 4. Design Decisions

### 4.1. Separating Query Metrics from Table Accesses

Query execution metrics are stored in `FACT_QUERY`, while table access relationships are stored in `BRIDGE_QUERY_TABLE`.

This separation is necessary because one query may access several tables. Joining the two tables produces multiple rows for such a query and can duplicate its resource consumption measures.

Consequently:

- Query counts and resource consumption are calculated from `FACT_QUERY`.
- Table usage analysis uses `BRIDGE_QUERY_TABLE`, joined to `FACT_QUERY` when user, team, or execution time information is needed.
- Table metadata is retrieved by joining `BRIDGE_QUERY_TABLE` to `DIM_TABLE`.

### 4.2. Possible Future Improvements

A future improvement would be to use a stable table identifier obtained from Trino event metadata or catalog metadata, if one is available and reliable for the required use case.

Such an identifier could preserve table identity across renames or changes in schema and catalog, depending on the guarantees provided by the source. Additional table attributes could also be incorporated into `DIM_TABLE`, `DIM_DATE`, `DIM_USER` and `DIM_TEAM`, but that is not relevant for now.