# SignalFlow X shortcuts (lite and stream profiles). Requires .env (copy .env.example).
SHELL := /bin/bash
include .env
export

DATA ?= data/seed7
SEED ?= 7

.PHONY: setup up down generate run status ui test test-unit test-integration lint clean \
	stream-up stream-down stream-init stream-replay stream-silver stream-transform stream-status test-stream

setup:            ## create venv and install
	python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev,ui,stream]"

up:               ## start PostgreSQL (lite profile)
	docker compose --profile lite up -d --wait

down:
	docker compose --profile lite down

generate:         ## deterministic dataset
	.venv/bin/signalflow generate --out $(DATA) --seed $(SEED) --overwrite

run:              ## bronze -> silver -> dbt gold
	.venv/bin/signalflow run --data $(DATA)

status:
	.venv/bin/signalflow status

ui:               ## control tower on http://127.0.0.1:8501 (needs `make run` first)
	.venv/bin/streamlit run src/signalflow/ui/app.py

lint:
	.venv/bin/ruff check src tests && .venv/bin/mypy src

test-unit:
	.venv/bin/pytest tests/unit -q

test-integration:
	.venv/bin/pytest tests/integration -q -rA

test: lint test-unit test-integration

# --- stream profile: Kafka (KRaft) + Kafka Connect/Debezium + Spark Structured Streaming (Java 17+ on the host) ---
stream-up:        ## fetch pinned jars/plugins (checksummed), build Connect, start Postgres + Kafka + Connect
	scripts/fetch_artifacts.sh && docker compose --profile stream up -d --build --wait

stream-down:
	docker compose --profile stream down

stream-init:      ## stream database, ERP, topics, Debezium connectors (idempotent)
	.venv/bin/signalflow stream init --data $(DATA)

stream-replay:    ## telecom events -> Kafka, manufacturing events -> ERP outbox -> Debezium -> Kafka
	.venv/bin/signalflow stream replay --data $(DATA) --pace-ms $(or $(PACE_MS),200)

stream-silver:    ## Spark streaming job (Ctrl-C to stop; restart resumes from its checkpoint)
	.venv/bin/signalflow stream silver

stream-transform: ## dbt gold on the streaming database
	.venv/bin/signalflow stream transform

stream-status:
	.venv/bin/signalflow stream status

test-stream:      ## Phase 2 acceptance tests (needs `make stream-up`)
	.venv/bin/pytest tests/stream -q -rA

clean:
	rm -rf data dbt/target dbt/logs
