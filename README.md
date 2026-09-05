# tailgate

**A Go read gateway that reduces straggler latency while bounding duplicate work.**

Tailgate sends a read to one replica and may launch one additional attempt when
the first is slow or fails. It returns the first complete response below HTTP 500,
cancels the losing attempt, and limits the extra load with admission credits.
Independent request and attempt limits prevent an unbounded hedge storm.

In the checked-in **500-request loopback experiment**, p99 fell from **43.3 ms to
12.3 ms**, with **1.208 backend attempts per request**. These are results from a
controlled synthetic straggler workload, not a production throughput claim.
[Read the setup, raw results, and limitations](docs/BENCHMARKS.md).

Systems research project, built with AI assistance. No external Go dependencies.

## Run the experiment

Requires Go 1.23+.

```sh
go test -race ./...
go vet ./...
go run ./cmd/experiment -requests 500
go test ./hedge -run '^$' -bench . -benchmem
```

The experiment starts two temporary HTTP replicas on loopback. Requests alternate
primaries, and specified request IDs inject a slow primary with a fast alternate.
Every returned body is checked. It prints baseline and hedged latency percentiles
and actual amplification as JSON Lines, then shuts the servers down.

## Run the gateway

```sh
go run ./cmd/tailgate \
  -backends http://127.0.0.1:8081,http://127.0.0.1:8082 \
  -listen 127.0.0.1:8080 \
  -admin 127.0.0.1:9090 \
  -hedge-delay 25ms -hedge-fraction 0.1 \
  -max-requests 128 -max-attempts 160

# From another terminal, with your replicas running:
curl http://127.0.0.1:8080/read
curl http://127.0.0.1:9090/metrics
```

Health and JSON metrics use a separate admin listener. The executable defaults to
loopback, imposes header/write timeouts, and drains on SIGINT/SIGTERM. `/healthz`
reports process liveness, not backend availability.

## Guarantees worth inspecting

| Property | Mechanism |
| --- | --- |
| At most two attempts per admitted request | One primary, one alternate, distinct configured URL |
| Aggregate hedge budget | `hedges <= initial_burst + fraction * admissions` |
| Global concurrency bounds | Separate nonblocking request and attempt semaphores |
| No unsafe method retries | GET/HEAD only, no request bodies |
| Circuit recovery | One half-open probe; generation tickets reject stale outcomes |
| Losing work cleanup | Shared cancellation, bounded response buffer, buffered result channel |
| Bounded response size | Read at most configured limit + 1 byte before rejecting |
| Replica health | Transport errors and 5xx count as failures; cancellation is neutral |

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway
    participant A as Primary
    participant B as Alternate
    C->>G: Read request
    G->>A: Attempt with shared deadline
    Note over G: Hedge delay expires; credits and capacity available
    G->>B: One duplicate read
    B-->>G: Complete bounded response
    G-->>A: Cancel losing attempt
    G-->>C: Return response
```

## Evidence

The gateway package reached **96.7% statement coverage** in the recorded local
race-enabled run. Tests cover overload, global concurrency, aggregate
amplification, cancellation, deadlines, real HTTP, response limits, escaped paths,
hop-by-hop headers, and circuit generations. This percentage covers the `hedge`
package; it does not describe executable entrypoint coverage.

- [Concurrency and failure design](docs/DESIGN.md)
- [Gateway tests](hedge/gateway_test.go)
- [Circuit and budget tests](hedge/breaker_test.go)
- [Review guide](docs/REVIEW.md)

## Deployment boundaries

Replicas must implement equivalent, side-effect-free GET/HEAD semantics. A GET
endpoint that changes state is unsuitable. Hedging does not create strong read
consistency across stale replicas. Responses are buffered, so this gateway is not
for token streaming, WebSockets, SSE, large downloads, or arbitrary write traffic.

There is no authentication, TLS termination, per-tenant admission, or production
deployment configuration. Headers (including authorization) go to configured
replicas; choose those destinations deliberately. Custom transports must obey
request context cancellation. Extra requests have a cost even when cancelled.

The design is motivated by the tradeoffs discussed in Google's
[The Tail at Scale](https://research.google/pubs/the-tail-at-scale/).
It does not reproduce that paper's experiments or claim their results.

MIT licensed. Companion projects: [price-time](https://github.com/yafi-s/price-time)
and [strata-journal](https://github.com/yafi-s/strata-journal).
