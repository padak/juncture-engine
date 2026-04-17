# Contributing to Juncture

## Local development setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,pandas]"
pytest
```

All contributions must:

1. Pass `pytest`.
2. Pass `ruff check src tests`.
3. Pass `mypy src` (strict mode).
4. Include tests for new behavior (unit + at least one integration test
   for adapter-level changes).

## Commit style

Commits use [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`. No AI
attribution footers; the commit describes the change, not the author.

## Filing issues

Before filing, check if the behavior is documented in
[`docs/DESIGN.md`](docs/DESIGN.md) or [`docs/ROADMAP.md`](docs/ROADMAP.md).
Include:

- Juncture version (`juncture --version`).
- DuckDB version (`duckdb --version`).
- Minimum reproducible project (zip the `juncture.yaml` + `models/`).
- Exact CLI invocation and full output.

## RFCs

Larger changes (new adapter, new major feature) go through a short RFC:

1. Create `docs/rfcs/NNNN-short-title.md` (copy the template when one
   exists).
2. Describe motivation, design, alternatives, risks, backward compat.
3. Open a PR; discuss; merge once accepted.
