.PHONY: help install fmt lint typecheck test test-fast examples clean

help:
	@echo "Targets:"
	@echo "  install     - pip install -e .[dev,pandas] in .venv"
	@echo "  fmt         - ruff format"
	@echo "  lint        - ruff check"
	@echo "  typecheck   - mypy src"
	@echo "  test        - pytest (full)"
	@echo "  test-fast   - pytest -x --ff"
	@echo "  examples    - run simple + ecommerce examples with tests"
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

clean:
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache
	find . -name '__pycache__' -type d -exec rm -rf {} +
	find examples -name '*.duckdb*' -delete
	find . -name 'target' -type d -exec rm -rf {} +
