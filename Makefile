.PHONY: help install fmt lint typecheck test test-fast examples e2e-test e2e-web clean

help:
	@echo "Targets:"
	@echo "  install     - pip install -e .[dev,pandas] in .venv"
	@echo "  fmt         - ruff format"
	@echo "  lint        - ruff check"
	@echo "  typecheck   - mypy src"
	@echo "  test        - pytest (full)"
	@echo "  test-fast   - pytest -x --ff"
	@echo "  examples    - run simple + ecommerce examples with tests"
	@echo "  e2e-test    - generate fake data for examples/eu_ecommerce,"
	@echo "                run the project end-to-end (small scale), and verify tests pass"
	@echo "  e2e-web     - same as e2e-test but also starts the web UI on localhost:8765"
	@echo "  clean       - remove build artifacts and DuckDB files"

install:
	python -m venv .venv
	.venv/bin/pip install -U pip
	.venv/bin/pip install -e '.[dev,pandas]'
	.venv/bin/pip uninstall -y sqlglotrs || true

fmt:
	.venv/bin/ruff format src tests

lint:
	.venv/bin/ruff check src tests

typecheck:
	.venv/bin/mypy src

test:
	.venv/bin/pytest -v --cov=juncture --cov-report=term-missing

test-fast:
	.venv/bin/pytest -x --ff

examples:
	cd examples/simple    && mkdir -p data && ../../.venv/bin/juncture run --project . --test
	cd examples/ecommerce && mkdir -p data && ../../.venv/bin/juncture run --project . --test

# End-to-end demo against the fictional "Fjord & Fable" EU e-commerce project.
# Generates deterministic fake data (seed=42), runs all 16 models, checks 57
# data tests. Uses the `small` scale so CI completes in seconds.
e2e-test:
	cd examples/eu_ecommerce && mkdir -p data && ../../.venv/bin/python scripts/generate_data.py --scale small
	.venv/bin/juncture run --project examples/eu_ecommerce --test --threads 4
	@echo ""
	@echo "--- Disable-toggle smoke test ---"
	.venv/bin/juncture run --project examples/eu_ecommerce --disable daily_revenue_anomalies --no-test
	@echo ""
	@echo "--- Run history sanity ---"
	@test -s examples/eu_ecommerce/target/run_history.jsonl && echo "run_history.jsonl populated"
	@echo ""
	@echo "e2e-test complete. Open http://127.0.0.1:8765 via 'make e2e-web' to inspect the DAG."

# Same as e2e-test but also boots the web UI. Blocks until Ctrl-C.
e2e-web: e2e-test
	@echo ""
	@echo "Starting web UI... open http://127.0.0.1:8765 in your browser."
	.venv/bin/juncture web --project examples/eu_ecommerce

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache
	find . -name '__pycache__' -type d -exec rm -rf {} +
	find examples -name '*.duckdb*' -delete
	find . -name 'target' -type d -exec rm -rf {} +
