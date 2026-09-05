# Request lifecycle and overload behavior

## Admission and amplification

Request admission is nonblocking: a full request semaphore returns 503 rather than
adding queueing latency. Each admitted request earns `fraction` credits, capped at
the burst capacity. Launching a hedge costs one credit. Idle time earns no credits.
Starting with `B` credits and admitting `N` requests therefore bounds hedges by
`B + f*N`; clamping credits can only make the bound tighter. Fraction zero disables
all extra attempts, including early failure fallback.

The same budget applies to a timer-triggered hedge and an early-error alternate.
A separate semaphore bounds active upstream calls across all clients, including
response body reads. If capacity or budget is unavailable at the one extra-attempt
decision, the request continues without a hedge; it does not queue or poll for one.
With many open circuits, request admission can still occur without launching a
primary. Metrics distinguish admissions, actual attempts, and rejected requests;
the hedge budget bound is defined against admissions, not successful reads.

## Response ownership

Each attempt reads and closes its own response body. The first complete response
below status 500 wins, including a 4xx response. A 5xx or transport/body error can
trigger the alternate immediately. If both attempts fail, the last completed 5xx
response is forwarded, or a generic 502 is returned for a transport/body failure.

Reading bodies before choosing a winner means slow response bodies are included
in the straggler policy. It also avoids cancelling the winner's context while its
body is still streaming. At most two results can be produced, and the channel has
capacity two: a loser can send after the handler returns without a drain goroutine.
Each goroutine releases its attempt permit on exit.

The request context shares the caller's cancellation and an additional total
upstream timeout. The built-in transport honors cancellation; arbitrary custom
transports must do so too. `timeouts` currently counts deadline/cancellation exits
through the handler's timeout path. A cancellation is never counted as a backend
health failure. Timers and goroutine scheduling are not real-time guarantees.

Memory is bounded by configured concurrency and response size, not just active
network calls. Completed buffers can remain attached to admitted handlers after
attempt permits are released. A conservative payload budget is approximately
`(MaxAttempts + 2*MaxRequests) * MaxResponseBytes`, plus Go/HTTP overhead, transient
buffer-growth copies, headers, and transport buffers. Choose limits together.

## Circuit generations

Failures accumulate until the configured threshold opens a replica circuit.
After cooldown, only one probe is admitted. Success closes the circuit, failure
reopens it, and cancellation releases the probe without a health verdict. Every
state transition increments a generation. Results from earlier generations are
ignored, preventing a slow pre-failure success from reopening traffic prematurely.

Replica selection rotates the starting position per admission and scans for a
closed/eligible circuit. Distinct URL strings are required; two URLs can still
resolve to the same physical service. There is no service discovery or topology
awareness in this implementation.

## HTTP boundaries

The gateway accepts bodyless GET/HEAD only. It rewrites the upstream host to the
configured replica, preserves the escaped path and query, removes hop-by-hop
headers including headers named by `Connection`, and buffers bounded responses.
It uses `RoundTrip` directly, so redirects are returned rather than followed.
The default transport does not inherit the process proxy environment. Unless the
caller supplies an encoding, `Accept-Encoding: identity` avoids transparent
decompression defeating the encoded response-byte limit.

Backends are trusted configuration. Authentication headers are forwarded and are
not logged. This is a read gateway, not a general-purpose hardened reverse proxy.
The admin listener has no access control and should stay on a trusted interface.

## Further experiments

Adaptive hedge thresholds need latency estimates that handle censored/cancelled
samples. Open-loop load generation is necessary to evaluate queue buildup and
coordinated omission. Per-tenant budgets require a fairness policy, not just a
global semaphore. These are future work; none is claimed by the current benchmark.
