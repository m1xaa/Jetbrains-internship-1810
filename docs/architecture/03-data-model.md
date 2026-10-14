# Meta DWH Data Model

## 1. Overview

The Meta Data Warehouse (DWH) is designed to analyze how Trino query executions use data tables and consume computational resources over time.

The model consists of two fact-like tables and three dimensions:

- `FACT_QUERY` — stores information about query executions and their resource consumption.
- `FACT_QUERY_TABLE_ACCESS` — records which tables were accessed by each query.
- `DIM_DATE` — provides a date dimension for time-based analysis.
- `DIM_USER` — stores user information.
- `DIM_TEAM` — stores team information.

The model separates query execution metrics from table access relationships. This allows us to analyze resource consumption without double-counting queries that access multiple tables.

## 2. Table Granularity and Structure

### 2.1. FACT_QUERY

**Granularity:** One row per completed query execution.

This table is the central fact table of the model. It stores the resource consumption metrics and identifies the user, team, and date associated with each execution.

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

Team information is stored separately from user information to avoid repeating team names for every user or query execution. Also each record of fact_query table has a team_id field so that if a user changes team, the record will still be associated with the right team.


### 2.5. FACT_QUERY_TABLE_ACCESS

**Granularity:** One row per query execution and accessed table.

This table records the relationship between query executions and the tables they access.

| Column | Description |
|---|---|
| `query_id` | Identifier of the query execution. Foreign key referencing `FACT_QUERY`. |
| `catalog_name` | Name of the catalog containing the accessed table. |
| `schema_name` | Name of the schema containing the accessed table. |
| `table_name` | Name of the accessed table. |

A query may access multiple tables, so a single `query_id` can appear in multiple rows.

The table name alone is not necessarily unique. Storing the catalog, schema, and table names together allows tables with identical names in different schemas or catalogs to be distinguished.

The combination of `query_id`, `catalog_name`, `schema_name`, and `table_name` identifies a query-table access relationship, assuming each table is recorded only once per query.

This table is fact-like because it records an observed relationship at a defined granularity. More specifically, it acts as a bridge between query executions and accessed tables. It does not contain resource consumption measures.

The table access information should preferably come from reliable Trino metadata. If it must be extracted from SQL text, the transformation needs to account for constructs such as aliases, common table expressions, nested queries, views, and partially qualified table names.

## 3. Relationships

The tables are connected through the following relationships:

```text
                      DIM_DATE
                         |
                      date_id
                         |
DIM_USER ---user_id--- FACT_QUERY ---team_id--- DIM_TEAM
                         |
                      query_id
                         |
               FACT_QUERY_TABLE_ACCESS
```

The relationships are:

- `FACT_QUERY.date_id` references `DIM_DATE.date_id`.
- `FACT_QUERY.user_id` references `DIM_USER.user_id`.
- `FACT_QUERY.team_id` references `DIM_TEAM.team_id`.
- `FACT_QUERY_TABLE_ACCESS.query_id` references `FACT_QUERY.query_id`.

Each query execution is associated with one user, one team at execution time, and one date. It may be associated with multiple accessed tables.

## 4. Design Decisions

### 4.1. Separating Query Metrics from Table Accesses

Query execution metrics are stored in `FACT_QUERY`, while table access relationships are stored in `FACT_QUERY_TABLE_ACCESS`.

This separation is necessary because one query may access several tables. Joining the two tables produces multiple rows for such a query and can duplicate its resource consumption measures.

Consequently:

- Query counts and resource consumption are calculated from `FACT_QUERY`.
- Table usage analysis uses `FACT_QUERY_TABLE_ACCESS`, joined to `FACT_QUERY` when user, team, or execution time information is needed.

### 4.2. User-to-Team Mapping

Each query execution is associated with the team the user belonged to at execution time.

If users can change teams, the transformation needs historical membership information to assign executions correctly. A mapping containing only each user's current team is insufficient for accurate historical attribution.

The availability and structure of this metadata must be verified before implementation.

### 4.3. Table Identification

The current model identifies accessed tables using `catalog_name`, `schema_name`, and `table_name`.

A future improvement would be to use a stable table identifier obtained from Trino event metadata or catalog metadata, if one is available and reliable for the required use case. This could improve table identity management and support additional table attributes through a dedicated `DIM_TABLE`.

This improvement is not required for the initial implementation.

### 4.4. Source Metadata Assumptions

The proposed model assumes that the source provides one final record per completed query execution, with a stable identifier such as `query_id`.

The actual source schema must be verified to confirm:
- Which query execution metrics are available.
- Whether the source provides reliable table access metadata.
- Whether user and team metadata are available.
- Whether historical team membership can be reconstructed.

Any unavailable fields or relationships will require additional metadata sources or transformations.