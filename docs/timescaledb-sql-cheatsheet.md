# TimescaleDB SQL Query Cheatsheet

Handy SQL patterns for working with time-series data in TimescaleDB.

---

# Time bucketing & aggregation

Group data into fixed intervals with `time_bucket`.

```sql
-- Average value per 1 hour bucket
SELECT time_bucket('1 hour', time) AS bucket, avg(value) AS avg_val
FROM sensor_data
GROUP BY bucket
ORDER BY bucket;
```

```sql
-- Max value per 5 minutes
SELECT time_bucket('5 minutes', time) AS bucket, max(value)
FROM sensor_data
GROUP BY bucket;
```

---

# Gap-filling

Fill missing time buckets using `time_bucket_gapfill`.

```sql
SELECT time_bucket_gapfill('1 hour', time) AS bucket,
       avg(value) AS avg_val
FROM sensor_data
WHERE time > now() - interval '1 day'
GROUP BY bucket
ORDER BY bucket;
```

Add `fill()` to control gap behavior (e.g. `fill(value=>0)`).

---

# First/last values

Get the first and last values per interval.

```sql
SELECT time_bucket('1 day', time) AS day,
       first(value, time) AS first_val,
       last(value, time) AS last_val
FROM sensor_data
GROUP BY day
ORDER BY day;
```

---

# Distinct counts

Efficient approximations on large datasets.

```sql
SELECT approx_count_distinct(sensor_id)
FROM sensor_data;
```

---

# Data retention & compression

Drop old chunks (manual retention).

```sql
SELECT drop_chunks('sensor_data', INTERVAL '90 days');
```

Enable compression policy (compress after 7 days).

```sql
ALTER TABLE sensor_data SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id'
);

SELECT add_compression_policy('sensor_data', INTERVAL '7 days');
```

---

# Size monitoring

Check hypertable size.

```sql
SELECT hypertable_size('sensor_data');
```

List all hypertables with sizes.

```sql
SELECT table_name, hypertable_size(table_name::regclass) AS size
FROM information_schema.tables
WHERE table_schema = 'public';
```

---

# Performance helpers

- Use `time_bucket` + `GROUP BY` instead of `date_trunc` for speed.
- Create indexes on `(time, sensor_id)` for faster queries.
- Use compression policies to reduce storage cost.
- Use `parallel_workers` for faster large aggregations.
