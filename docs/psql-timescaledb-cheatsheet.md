# PostgreSQL + TimescaleDB `psql` Cheatsheet

Handy reference for exploring and working with TimescaleDB using the `psql` shell.

---

# Restart PSQL service

```sh
brew services stop postgresql
brew services start postgresql@17
pg_isready
psql -U postgres -d dts_db

```

---

# Connection / Exit

- `psql -U postgres -d mydb` — connect to database `mydb`
- `psql -U postgres -d mydb -X` — connect **without** loading `~/.psqlrc` (useful for extension upgrades)
- `\q` — quit

---

# Explore databases & schemas

- `\l` — list all databases
- `\c mydb` — connect to database `mydb`
- `\dn` — list schemas

---

# Tables & extensions

- `\dt` — list tables in current schema
- `\d tablename` — describe table (columns, indexes, constraints)
- `\dx` — list installed extensions
- `\dx+` — list extensions with more detail

---

# Query helpers

- `\timing` — show how long queries take
- `\x` — toggle expanded display (useful for wide rows)
- `\! clear` — clear terminal screen
- `\e` — open the last query in your editor (`$EDITOR`)
- `\p` — show the current query buffer (what you’ve typed but not executed)
- `\r` — reset/clear the query buffer

---

# TimescaleDB-specific

- View hypertable/index/storage details:
  ```
  \d+ sensor_data
  ```
- Show TimescaleDB extension version:
  ```
  \dx timescaledb
  ```
- Example: size of a hypertable
  ```sql
  SELECT hypertable_size('sensor_data');
  ```
- Example: bucket data by 1 hour and compute average
  ```sql
  SELECT time_bucket('1 hour', time) AS bucket, avg(value) AS avg_value
  FROM sensor_data
  GROUP BY bucket
  ORDER BY bucket;
  ```

---

# Useful TimescaleDB functions & patterns

- `time_bucket(bucket_interval, timestamp)` — aggregate timestamps into regular buckets.
  ```sql
  SELECT time_bucket('5 minutes', time) AS bucket, max(value)
  FROM sensor_data
  GROUP BY bucket;
  ```
- `time_bucket_gapfill(interval, timestamp)` — like `time_bucket` but fills gaps (useful with `ORDER BY` and `fill()` in `gapfill` queries).
  ```sql
  SELECT time_bucket_gapfill('1 hour', time) AS hour,
         avg(value) FROM sensor_data
  WHERE time > now() - interval '1 day'
  GROUP BY hour
  ORDER BY hour;
  ```
- `first(value, time)` / `last(value, time)` — get first/last value in an aggregation window.
  ```sql
  SELECT time_bucket('1 day', time) AS day,
         first(value, time) AS first_val,
         last(value, time) AS last_val
  FROM sensor_data
  GROUP BY day;
  ```
- `approx_count_distinct(expr)` — fast approximate distinct counts for large datasets.
  ```sql
  SELECT approx_count_distinct(sensor_id) FROM sensor_data;
  ```
- Compression / hypertable maintenance:
  - Create compression policy or compress chunks (see docs for steps); basic functions include `compress_chunk(chunk_name)` and `add_compression_policy('hypertable', compress_after => interval '7 days')`.
- Monitoring/size helpers:
  ```sql
  SELECT table_name, hypertable_size(table_name::regclass)
  FROM (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') t;
  ```

---

# Quick tips

- When upgrading TimescaleDB, start `psql` with `-X` and run `ALTER EXTENSION timescaledb UPDATE;` as the **first** command.
- Use `\timing` to measure query performance while you iterate on SQL.
- For bulk inserts, use `COPY` or batched `INSERT` to keep insert throughput high.
