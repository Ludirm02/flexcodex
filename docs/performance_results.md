# FlexQL Performance Results

## Environment

- Date: March 19, 2026
- Machine: local development machine (Linux)
- Build: `make -j$(nproc)`
- Script: `./scripts/run_benchmark.sh <port> <rows> <out-file>`
- Assignment-scale command: `./scripts/run_benchmark.sh 9000 10000000`

## Raw Benchmark Output (Latest Post-Optimization Runs)

### 100K rows

```text
rows_inserted=100000
insert_total_seconds=0.192248
insert_rows_per_second=520162
point_queries=5000
point_query_total_seconds=0.120929
point_query_avg_ms=0.0241857
full_scan_rows_returned=50000
full_scan_seconds=0.0280519
cached_query_rows_first=20000
cached_query_rows_second=20000
cached_query_first_seconds=0.00464554
cached_query_second_seconds=0.00155936
```

### 1M rows

```text
rows_inserted=1000000
insert_total_seconds=1.66257
insert_rows_per_second=601478
point_queries=5000
point_query_total_seconds=0.130255
point_query_avg_ms=0.026051
full_scan_rows_returned=500000
full_scan_seconds=0.296228
cached_query_rows_first=200000
cached_query_rows_second=200000
cached_query_first_seconds=0.0603823
cached_query_second_seconds=0.0208144
```

## Notes

- Point-query timings reflect primary-index lookup (`WHERE id = ...`).
- Insert throughput improved significantly due larger batched multi-row insert statements and reduced insert-side numeric re-parsing.
- Full-scan/cached query timings improved from lower-allocation protocol serialization/parsing and numeric fast-path execution.
- For submission, run and attach the full 10M output file from `scripts/run_benchmark.sh`.
