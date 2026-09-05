# Review latency as a resource tradeoff

Start with the experiment. Count which request IDs receive a slow primary and
which alternate will answer quickly. Predict amplification before looking at the
results. Then make both replicas slow for the same IDs: correlated stragglers
should remove much of the benefit while retaining duplicate work.

Explain why a global in-flight cap alone does not bound total extra requests over
time. Derive the credit inequality from initial balance, credits earned, and
credits spent. Then distinguish its admission denominator from successful requests.

Trace the successful alternate path and identify who owns each response body,
which context is cancelled, and why the losing goroutine cannot block sending its
result. Explain why picking a winner on response headers would change the design.

Read the circuit generation test. Reconstruct the race where an old success
arrives after newer failures have opened the circuit. Explain why a mutex alone
does not prevent that logically stale update.

For an extension, implement an open-loop experiment with scheduled send times and
include queue delay in latency. Compare baseline and hedged policies at equal
offered load, show rejected requests, and report amplification alongside p99.

Reproduce the experiment and implement an extension you understand before using
the project to substantiate interview claims.
