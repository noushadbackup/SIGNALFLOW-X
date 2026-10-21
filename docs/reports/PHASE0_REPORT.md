# Phase 0 Report — Research and Architecture

Date: 2026-10-21. Environment: cloud Linux container, 4 vCPU, 15 GB RAM, Python 3.12 venv, Docker 29.9 daemon.

## Delivered

| Artifact | Path |
|----------|------|
| Requirements (business, platform, delivery semantics, resource envelope, "production ready" definition) | `docs/REQUIREMENTS.md` |
| Architecture, layer contracts, processing semantics, dimensional model, risk register | `docs/ARCHITECTURE.md` |
| Data contracts explained (envelope, 18 event types / 19 versions, evolution, quarantine, lateness) | `docs/DATA_CONTRACTS.md` |
| Machine-readable contracts (JSON Schema 2020-12) | `contracts/` |
| Contract registry + validation policy + compatibility checker | `src/signalflow/contracts.py` |
| ADRs 0001–0007 | `docs/adr/` |
| Implementation plan with MVP, critical path, phase acceptance criteria | `docs/IMPLEMENTATION_PLAN.md` |
| Test strategy | `docs/TEST_STRATEGY.md` |

## Workspace audit (facts measured, not assumed)

* Empty workspace, no existing repository attached.
* Docker daemon had to be started manually; Docker Hub returned HTTP 429 (rate limit). Images resolve through
  `mirror.gcr.io`. A PostgreSQL 16.15 container started and answered queries.
* `quay.io` is blocked by the environment proxy (affects Debezium 3.x images). `minio/minio` tags did not resolve
  through the mirror. Maven Central rate-limited during checks. These are environment limits, recorded in ADR-0005
  with the chosen fallbacks.

## Test results (real run)

```
$ pytest tests/unit -q
74 passed in 0.22s
$ ruff check src tests
All checks passed!
$ mypy src            # strict mode
Success: no issues found in 2 source files
```

The 74 tests cover: every contract has a valid sample and accepts it; every contract rejects an unknown field and a
missing required field; amount type/sign/precision rules; recharge status ↔ failure_reason rule; usage unit ↔ type
rule; zero inventory delta; v1.1 optional field; unregistered major version; unknown event type; six envelope
violations; clock-skew check; minor-version compatibility across all contracts; the compatibility checker detecting
a breaking change; canonical formatting of contract files.

## Known limitations

* Stack versions for Phases 2–3 (Spark/Iceberg pairing, Debezium, object storage) are chosen with partial evidence;
  a compatibility spike at the start of Phase 2 is required before streaming code is written.
* RAM figures per profile are estimates until measured in Phases 2–3.
* No CI yet (added in Phase 1).

## How to verify

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
pytest tests/unit -q
```
