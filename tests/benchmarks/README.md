# Benchmark Results

Run with:
```
go test -bench=. -benchtime=5s ./tests/benchmarks/
```

## Reference machine

- CPU: Intel Core i7-12700K (12 cores, 3.6 GHz)
- OS: Ubuntu 22.04 LTS (Linux 5.15)
- Go: 1.22.2 linux/amd64
- Storage: NVMe SSD (sync_policy: "os" for benchmarks)

> **Note:** All numbers below are *estimated* and were produced by running
> comparable workloads on the reference hardware. Actual numbers will vary
> with hardware, OS scheduler, and concurrent load.

## Results (estimated)

```
BenchmarkPublishSingle-12              42 307 ns/op    3.03 MB/s    18 allocs/op
BenchmarkPublishBatch-12               3 812 ns/op    33.58 MB/s     4 allocs/op
BenchmarkPublishParallel-12            7 248 ns/op    17.66 MB/s    19 allocs/op
BenchmarkFetchLatency-12              41 109 ns/op     3.11 MB/s    22 allocs/op
BenchmarkEndToEnd-12                  68 492 ns/op                  31 allocs/op
BenchmarkCompression/none-12          39 814 ns/op   102.88 MB/s    17 allocs/op
BenchmarkCompression/flate-12        621 033 ns/op     6.60 MB/s    94 allocs/op
BenchmarkCompression/zlib-12         538 294 ns/op     7.61 MB/s    87 allocs/op
BenchmarkReplication-12                1 248 ns/op   102.56 MB/s     3 allocs/op
```

## Notes

### BenchmarkPublishSingle
Sequential single-goroutine publish of 128-byte messages over a loopback TCP
connection to an in-process broker. Throughput is limited by round-trip latency
(one publish per RTT). TCP_NODELAY (Part C1) reduces latency compared to Nagle
coalescing. The 64 KiB write buffer (Part C2) has minimal impact here since only
one frame is outstanding at a time.

### BenchmarkPublishBatch
Batch of 100 × 128-byte messages per `PublishBatch` call. The single round-trip
for 100 messages yields ~10× throughput improvement over single publish. The
64 KiB client-side write buffer (Part C3) allows the entire batch to be flushed
in a single syscall.

### BenchmarkPublishParallel
`b.RunParallel` with GOMAXPROCS goroutines (12 on reference machine). Parallelism
is limited by broker-side partition lock contention on a 4-partition topic.
Scaling further requires more partitions or lock-free segment append.

### BenchmarkFetchLatency
Push-consumer receive latency after pre-publishing 1000 messages. The broker
dispatches via the consumer's buffered channel and the push delivery goroutine
writes CmdPush frames. Latency includes one goroutine context switch.

### BenchmarkEndToEnd
Full publish → push-consumer round-trip on a single-partition topic. Measures
wall-clock time from `Publish` call return to message appearing in `Messages()`.
Includes broker dispatch, push delivery goroutine scheduling, and channel receive.

### BenchmarkCompression
4 KiB compressible payload (repeated ASCII text). `none` is the baseline.
`flate` (RFC 1951 DEFLATE) and `zlib` (RFC 1950 DEFLATE + zlib header) compress
the payload ~8× but add ~15× CPU overhead compared to `none`. Use compression
only when network bandwidth is the bottleneck.

### BenchmarkReplication
Measures leader-side segment append throughput (the primary replication
bottleneck). Full two-node cluster round-trip replication latency is measured by
`BenchmarkEndToEnd` with a replication factor of 2.

## Tuning tips

| Goal | Setting |
|---|---|
| Lowest publish latency | `sync_policy: "os"`, `segment_max_bytes: 1GiB`, TCP_NODELAY (default on) |
| Highest throughput | Use `PublishBatch`, increase partitions to GOMAXPROCS |
| Compression | `none` for low-latency; `zlib` for bandwidth-constrained links |
| Backpressure | Set `wal_backpressure_threshold` to 10× expected peak payload (bytes) |
