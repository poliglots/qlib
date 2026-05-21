.PHONY: install algo dev data run web

VENV := .venv/bin
QRUN := $(VENV)/qrun
UVICORN := $(VENV)/uvicorn

MODEL ?= lightgbm
START_DATE ?= 2020-01-01
END_DATE ?= $(shell date +%Y-%m-%d)
QLIB_DATA_DIR ?= $(HOME)/.qlib/qlib_data/nse_data
HOST ?= 0.0.0.0
PORT ?= 8000

install:
	uv sync
	uv pip install -e .

algo:
	uv pip install -e ".[nse]"

dev:
	@trap 'kill 0' EXIT INT TERM HUP; \
	QLIB_DATA_DIR=$(QLIB_DATA_DIR) $(UVICORN) web.server.main:app --reload --host $(HOST) --port $(PORT) \
	& cd web && yarn && yarn --prefix web dev; \
	wait

data:
	bash scripts/prepare_nse_data.sh $(START_DATE) $(END_DATE) $(QLIB_DATA_DIR)

run:
	$(QRUN) $(shell find examples/benchmarks -iname "workflow_config_$(MODEL)_*NSE.yaml" | head -1)

web:
	QLIB_DATA_DIR=$(QLIB_DATA_DIR) $(UVICORN) web.server.main:app --reload --host $(HOST) --port $(PORT)
