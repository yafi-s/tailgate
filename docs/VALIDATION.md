# Verified implementation snapshot

The implementation passed [GitHub Actions run 33940345481](https://github.com/yafi-s/tailgate/actions/runs/33940345481) on 2026-09-04 (America/Chicago).

Formatting, vet, race tests, executable builds, and the HTTP experiment on Go 1.23 and 1.27.

The README badge tracks the current default branch. This pinned run records the
source validation at publication; future changes should be assessed against their
own checks. Local benchmark results and their limitations are documented in
[BENCHMARKS.md](BENCHMARKS.md).

A local smoke check also ran the compiled gateway against a real HTTP replica,
verified the separate health/metrics listener, and confirmed a clean SIGTERM exit.
