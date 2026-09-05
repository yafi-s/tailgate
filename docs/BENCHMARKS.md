# Controlled straggler experiment

Recorded 2026-09-04 (America/Chicago). Apple M2, macOS 26.5.2, Go 1.27.1,
darwin/arm64. Real HTTP over loopback, two temporary replicas, one sequential
closed-loop client, 500 verified responses per policy. One uncontrolled desktop
run, without CPU pinning or isolation from other processes.

| Policy | p50 | p95 | p99 | Backend attempts/request |
| --- | ---: | ---: | ---: | ---: |
| No extra attempts | 2.865 ms | 42.679 ms | 43.301 ms | 1.000 |
| 5 ms hedge delay; 25% earned credits | 2.820 ms | 10.451 ms | 12.320 ms | 1.208 |

The measured tradeoff is **about 72% lower p99 at 20.8% more backend calls** for
this workload. The configured bound is 25% plus an initial four-credit burst;
the experiment reports actual attempts rather than implying every hedge was free.

## Workload

Replica A sleeps 40 ms for IDs congruent to 0 modulo 10. Replica B sleeps 40 ms
for IDs congruent to 1 modulo 10. Otherwise both sleep 2 ms. Primaries alternate,
so 20% of baseline requests encounter a slow primary while their alternate is
fast. This deliberately favors hedging and isolates the latency/load tradeoff.
Correlated stragglers or saturated replicas can remove the benefit or make it worse.

Every response must be status 200 with the expected replicated value. Timing
includes client send, gateway scheduling, upstream HTTP, complete body read, and
client body validation/read overhead up to sample recording. The output contains
gateway counters so unexpected rejections or deadline failures are visible.

```sh
go run ./cmd/experiment -requests 500
go test ./hedge -run '^$' -bench . -benchmem
```

[Raw experiment output](benchmarks/local.jsonl).
[Fast-path microbenchmark output](benchmarks/fast-path.txt).

The microbenchmark uses a stub transport, so its 5,113 ns/op and 8,179 B/op
(38 allocations/op) include request/recorder construction and gateway work but no
real networking. It identifies optimization headroom; it is not a server capacity
estimate.

## What this does not establish

There is no offered-load sweep, multi-client fairness measurement, cold connection
study, WAN experiment, or production traffic trace. A closed-loop client omits
queue buildup under externally imposed load, so these percentiles must not be used
as an overload SLO. With 500 samples, p99 has limited statistical resolution.
No cross-project or paper result is used as a numeric baseline.

`go test -race` passed locally; `hedge` package coverage was 96.7% of statements.
CLI entrypoints are outside that percentage. `go vet ./...` passed.
