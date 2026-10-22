# SignalFlow X shortcuts (lite, stream, lake, airflow and superset profiles). Requires .env (copy .env.example).
SHELL := /bin/bash
include .env
export

DATA ?= data/seed7
SEED ?= 7

.PHONY: setup up down generate run status ui test test-unit test-integration lint clean \
	hunt cases evaluate operator \
	stream-up stream-down stream-init stream-replay stream-silver stream-transform stream-status test-stream \
	lake-up lake-down lake-publish lake-transform lake-compare lake-status lake-maintain test-lake \
	airflow-up airflow-password superset-up superset-bootstrap

setup:            ## create venv and install
	python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev,ui,stream,lake]"

up:               ## start PostgreSQL (lite profile)
	docker compose --profile lite up -d --wait

down:
	docker compose --profile lite down

generate:         ## deterministic dataset
	.venv/bin/signalflow generate --out $(DATA) --seed $(SEED) --overwrite \
		$(if $(DAYS),--days $(DAYS)) $(if $(SUBSCRIBERS),--subscribers $(SUBSCRIBERS))

run:              ## bronze -> silver -> dbt gold (Leak Hunter after each arrival file)
	.venv/bin/signalflow run --data $(DATA)

status:
	.venv/bin/signalflow status

ui:               ## control tower on http://127.0.0.1:8501 (needs `make run` first)
	.venv/bin/streamlit run src/signalflow/ui/app.py

# --- Revenue Leak Hunter (Phase 4); DB=<database> defaults to PGDATABASE ---
hunt:             ## run the ten rules once and move cases through their lifecycle
	.venv/bin/signalflow assurance hunt --database $(or $(DB),$(PGDATABASE))

cases:            ## open cases, most severe first
	.venv/bin/signalflow assurance cases --database $(or $(DB),$(PGDATABASE))

evaluate:         ## offline precision/recall against the scenario ledger of $(DATA)
	.venv/bin/python -m signalflow_eval.leak_hunter --data $(DATA) --database $(or $(DB),$(PGDATABASE))

operator:         ## create or update a sign-in: make operator OPERATOR=nadia ROLE=analyst NAME="Nadia Rahman"
	.venv/bin/signalflow operators add $(OPERATOR) --role $(or $(ROLE),viewer) $(if $(NAME),--name "$(NAME)")

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

# --- lake profile: SeaweedFS (S3 API) + Iceberg JDBC catalog in Postgres + Trino; Spark publisher on the host ---
lake-up:          ## fetch jars, start Postgres + SeaweedFS (+ bucket) + Trino
	scripts/fetch_artifacts.sh && docker compose --profile lake up -d --wait

lake-down:
	docker compose --profile lake down

lake-publish:     ## committed Postgres state -> Iceberg (incremental, idempotent); DB=<source database>
	.venv/bin/signalflow lake publish --database $(or $(DB),$(PGDATABASE))

lake-transform:   ## dbt build on Trino (Gold as Iceberg tables)
	.venv/bin/signalflow lake transform

lake-compare:     ## Gold on Postgres vs Gold on the lake, row by row
	.venv/bin/signalflow lake compare --database $(or $(DB),$(PGDATABASE))

lake-status:
	.venv/bin/signalflow lake status

lake-maintain:    ## compact files, expire snapshots (keeps the last 50)
	.venv/bin/signalflow lake maintain

test-lake:        ## Phase 3 acceptance tests (needs `make lake-up`)
	.venv/bin/pytest tests/lake -q -rA

airflow-up:       ## Airflow 3.1 (UI on http://127.0.0.1:58081, user admin, password printed by airflow-password)
	docker compose --profile lake --profile airflow up -d --build --wait

airflow-password:
	docker compose exec airflow-apiserver cat /opt/airflow/auth/passwords.json

superset-up:      ## Superset 5.0 on http://127.0.0.1:58088 (user admin, SUPERSET_ADMIN_PASSWORD from .env)
	docker compose --profile lake --profile superset up -d --build --wait

superset-bootstrap: ## Trino connection, datasets, charts and the dashboard, as code
	.venv/bin/signalflow bi bootstrap

clean:
	rm -rf data dbt/target dbt/logs
