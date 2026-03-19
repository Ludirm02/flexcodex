# FlexQL Design Document

Repository Link: `SET_THIS_TO_YOUR_ACTUAL_GITHUB_REPOSITORY_URL_BEFORE_SUBMISSION`

## 1. System Overview

FlexQL is implemented as a client-server database driver:

- **Server (`flexql_server`)**: maintains in-memory tables and executes SQL-like queries.
- **Client API (`flexql_client`)**: exposes required C/C++ APIs and communicates with server via TCP.
- **REPL (`flexql-client`)**: interactive terminal built using the client API.

The server is multithreaded and handles each client connection in a separate thread.

## 2. Storage Design

### 2.1 Logical Model

Each table stores:

- Table name
- Column definitions
- Primary key metadata
- Row data
- Row expiration timestamp
- Table version (for cache invalidation)

### 2.2 Physical In-Memory Representation

- **Row-major storage** is used.
- Each row is stored as:
  - `vector<string> values`
  - `array<long double, 64> numeric_cache` for pre-parsed numeric fast paths
  - `array<uint8_t, 64> numeric_valid` bitmap for numeric cache validity
  - `int64_t expires_at_unix`

This layout keeps insert and join projection logic simple while still enabling indexing and selective scans.

### 2.3 Schema Storage

For every table:

- `columns`: ordered vector of columns
- `column_index`: hash map `column_name -> column_position`
- `primary_key_col`: index of PK column (`-1` if none)

Identifiers are normalized to lowercase to provide case-insensitive SQL identifier handling.

## 3. SQL Execution Design

The engine supports:

- `CREATE TABLE`
- `INSERT INTO ... VALUES (...) [TTL <sec> | EXPIRES <unix|datetime>]`
- `INSERT INTO ... VALUES (...), (...), ... [TTL <sec> | EXPIRES <unix|datetime>]`
- `SELECT ... FROM ... [WHERE ...]`
- `SELECT ... FROM ... INNER JOIN ... ON ... [WHERE ...]`

### 3.1 WHERE Restriction

Only one simple condition is supported (`col op literal`) with operators:

- `=` `!=` `<` `<=` `>` `>=`

No `AND`/`OR` combinations are supported, matching assignment constraints.

### 3.2 Type Validation

Supported types:

- `INT`
- `DECIMAL`
- `VARCHAR(n)` / `VARCHAR`
- `DATETIME` (`YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DDTHH:MM:SS`)

Values are validated during insert and during typed comparisons in `WHERE`.

## 4. Primary Indexing

Primary indexing is implemented using:

- `unordered_map<string, size_t> primary_index`

Where key is PK value, and value is the row index.

Optimization used:

- For `SELECT ... WHERE primary_key = value`, engine uses index lookup instead of full scan.

This reduces equality lookup complexity from O(n) to expected O(1).

For non-primary numeric predicates, the engine also tracks numeric range index entries per numeric column. Queries with numeric `WHERE` operators (`=`, `<`, `<=`, `>`, `>=`) can use sorted range scans instead of scanning all rows.

## 5. Caching Strategy

FlexQL uses an **LRU query cache** for `SELECT` results.

Cache entry contains:

- Query result rows + columns
- Snapshot of involved table versions

### 5.1 Cache Key

Normalized SQL text (whitespace-collapsed and case-normalized outside literals).

### 5.2 Invalidation

Each table has a monotonic `version` incremented on inserts.

A cached result is valid only if all referenced table versions match current versions.

### 5.3 Benefits

Repeated identical read queries can be served without re-executing scans/joins.

## 6. Expiration Timestamp Handling

Each inserted row carries an expiration time:

- `TTL seconds` converts to `now + seconds`
- `EXPIRES` accepts unix epoch or datetime literal
- Missing TTL/EXPIRES means no expiration

Expired rows are filtered out at read time (`SELECT`, `JOIN`).

## 7. Concurrency and Multithreading

The server accepts multiple clients and spawns one thread per connection.

Concurrency control:

- `shared_mutex` for table state
- Shared lock for `SELECT`
- Unique lock for `CREATE/INSERT`
- Additional mutex inside LRU cache

This allows concurrent readers while keeping writes safe.

## 8. Network/API Design

Required APIs implemented:

- `flexql_open`
- `flexql_close`
- `flexql_exec`
- `flexql_free`

The database handle is opaque (`struct FlexQL` hidden in implementation).

Protocol is length-prefixed request + line-based response:

- Request: `Q <len>\n<sql-bytes>`
- Success: `OK <ncols>`, optional `COL`, repeated `ROW`, then `END`
- Error: `ERR\t<message>`

TCP `TCP_NODELAY` is enabled to reduce latency for many small SQL requests.
Client query send path transmits header/body without building a large combined packet copy.
Protocol row parsing and serialization paths are buffered and low-allocation for high-row-count scans.

## 9. Performance Evaluation Setup

Benchmark client (`flexql_benchmark`) measures:

- Insert time and throughput
- Indexed point-query latency
- Full-scan query latency
- Cached query first vs repeated execution time

Benchmark script:

- `scripts/run_benchmark.sh`

Recorded sample results are in `docs/performance_results.md`.

## 10. Build and Execution Instructions

Build:

```bash
make -j$(nproc)
```

Run server:

```bash
./build/flexql_server 9000
```

Run REPL:

```bash
./build/flexql-client 127.0.0.1 9000
```

PDF-style launchers also exist at repository root:

```bash
./flexql-server 9000
./flexql-client 127.0.0.1 9000
```

Run smoke test:

```bash
./build/flexql_smoke_test 127.0.0.1 9000
```

Run benchmark:

```bash
./scripts/run_benchmark.sh 9000 10000000
```
