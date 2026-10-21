# SignalFlow X — lite profile shortcuts. Requires .env (copy .env.example).
SHELL := /bin/bash
include .env
export

DATA ?= data/seed7
SEED ?= 7

.PHONY: setup up down generate run status test test-unit test-integration lint clean

setup:            ## create venv and install
	python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"

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

lint:
	.venv/bin/ruff check src tests && .venv/bin/mypy src

test-unit:
	.venv/bin/pytest tests/unit -q

test-integration:
	.venv/bin/pytest tests/integration -q -rA

test: lint test-unit test-integration

clean:
	rm -rf data dbt/target dbt/logs
