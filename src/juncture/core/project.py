"""Project: load a Juncture project from disk, discover models, produce a DAG.

A project directory typically looks like::

    my_project/
        juncture.yaml       # project config (connections, defaults)
        models/             # SQL + Python models
            staging/
                stg_orders.sql
                stg_customers.sql
            marts/
                customer_lifetime_value.sql
                churn_score.py
        tests/              # assertions (optional)
        macros/             # shared SQL snippets (optional)
        seeds/              # CSV seed data (optional)

Models are discovered by walking ``models/``. ``.sql`` files become SQL models
with filename (without extension) as the model name; dependencies are inferred
from ``ref()`` calls. ``.py`` files are imported and every ``@transform``
function is registered.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from juncture.core.dag import DAG
from juncture.core.decorators import get_metadata, is_transform
from juncture.core.model import ColumnSpec, Materialization, Model, ModelKind
from juncture.parsers.sqlglot_parser import extract_refs


class ProjectError(Exception):
    """Raised when a Juncture project cannot be loaded."""


_ENV_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _interpolate_env(obj: Any) -> Any:
    """Recursively replace ``${VAR}`` (optionally ``${VAR:-default}``) tokens.

    Unset variables without a default raise :class:`ProjectError`. Defaults
    are supported as ``${VAR:-fallback}``; empty string is an allowed value.
    """
    if isinstance(obj, str):

        def repl(match: re.Match[str]) -> str:
            name = match.group(1)
            default = match.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ProjectError(
                f"Environment variable ${{{name}}} is referenced in juncture.yaml "
                f"but not set; export it or add a ${{{name}:-default}} fallback."
            )

        return _ENV_VAR.sub(repl, obj)
    if isinstance(obj, dict):
        return {k: _interpolate_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_env(v) for v in obj]
    return obj


@dataclass(kw_only=True)
class ConnectionConfig:
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(kw_only=True)
class ProjectConfig:
    """Parsed ``juncture.yaml``."""

    name: str
    version: str = "0.1.0"
    profile: str = "default"
    connections: dict[str, ConnectionConfig] = field(default_factory=dict)
    models_path: Path = Path("models")
    tests_path: Path = Path("tests")
    macros_path: Path = Path("macros")
    seeds_path: Path = Path("seeds")
    default_materialization: Materialization = Materialization.TABLE
    default_schema: str = "main"
    model_defaults: dict[str, Any] = field(default_factory=dict)
    vars: dict[str, Any] = field(default_factory=dict)
    jinja: bool = False

    @classmethod
    def from_file(cls, path: Path) -> ProjectConfig:
        raw_text = path.read_text()
        raw = yaml.safe_load(raw_text)
        if not isinstance(raw, dict):
            raise ProjectError(f"{path}: expected a mapping at the top level")
        raw = _interpolate_env(raw)

        connections_raw = raw.get("connections", {})
        connections = {
            name: ConnectionConfig(type=cfg["type"], params={k: v for k, v in cfg.items() if k != "type"})
            for name, cfg in connections_raw.items()
        }

        def _p(key: str, default: str) -> Path:
            return Path(raw.get(key, default))

        default_mat = Materialization(raw.get("default_materialization", "table"))
        return cls(
            name=raw["name"],
            version=raw.get("version", "0.1.0"),
            profile=raw.get("profile", "default"),
            connections=connections,
            models_path=_p("models_path", "models"),
            tests_path=_p("tests_path", "tests"),
            macros_path=_p("macros_path", "macros"),
            seeds_path=_p("seeds_path", "seeds"),
            default_materialization=default_mat,
            default_schema=raw.get("default_schema", "main"),
            model_defaults=raw.get("model_defaults", {}),
            vars=raw.get("vars", {}),
            jinja=bool(raw.get("jinja", False)),
        )


@dataclass(kw_only=True)
class SeedSpec:
    """A seed (source table) loaded before the DAG runs.

    ``format`` is ``"csv"`` for a single ``.csv`` file, ``"parquet"`` for a
    directory of sliced ``.parquet`` files. In the parquet case ``path``
    points to the directory; DuckDB reads it via
    ``read_parquet('<path>/*.parquet')``.
    """

    name: str
    path: Path
    format: str = "csv"
    schema_overrides: dict[str, str] = field(default_factory=dict)


@dataclass(kw_only=True)
class CustomTestSpec:
    """A SQL file under tests/ that returns failing rows."""

    name: str
    path: Path
    sql: str


@dataclass(kw_only=True)
class Project:
    root: Path
    config: ProjectConfig
    models: list[Model] = field(default_factory=list)
    schemas: dict[str, dict[str, Any]] = field(default_factory=dict)
    seeds: list[SeedSpec] = field(default_factory=list)
    custom_tests: list[CustomTestSpec] = field(default_factory=list)

    @classmethod
    def load(cls, root: Path | str) -> Project:
        root = Path(root).resolve()
        _load_dotenv_if_present(root)
        config_path = root / "juncture.yaml"
        if not config_path.exists():
            raise ProjectError(f"No juncture.yaml in {root}")
        config = ProjectConfig.from_file(config_path)

        project = cls(root=root, config=config)
        project._load_schemas()
        project.seeds = project._discover_seeds()
        project.custom_tests = project._discover_custom_tests()
        project.models = project._discover_models()
        return project

    def dag(self) -> DAG:
        return DAG.from_models(self.models)

    def _load_schemas(self) -> None:
        models_root = self.root / self.config.models_path
        if not models_root.exists():
            return
        for schema_file in models_root.rglob("schema.yml"):
            data = yaml.safe_load(schema_file.read_text()) or {}
            for model_decl in data.get("models", []):
                self.schemas[model_decl["name"]] = model_decl

    def _discover_seeds(self) -> list[SeedSpec]:
        """Discover seeds under ``seeds_path``.

        Recognises two layouts:

        * ``seeds/<name>.csv`` -- a single CSV file.
        * ``seeds/<sub>/<name>/*.parquet`` -- a directory of Parquet slices.
          The seed name is the directory's relative path joined with ``.``
          (so ``seeds/in-c-db/carts/`` becomes seed ``in-c-db.carts``).

        Directory-based CSV is not supported; a lone ``.csv`` inside a
        directory is treated as its own seed using the file stem.
        """
        seeds_root = self.root / self.config.seeds_path
        if not seeds_root.exists():
            return []
        schema_overrides: dict[str, dict[str, str]] = {}
        schema_file = seeds_root / "schema.yml"
        if schema_file.exists():
            data = yaml.safe_load(schema_file.read_text()) or {}
            for seed in data.get("seeds", []):
                schema_overrides[seed["name"]] = seed.get("columns", {})

        specs: list[SeedSpec] = []
        # Parquet seeds: directories that contain one or more *.parquet files.
        for parquet_dir in sorted({p.parent for p in seeds_root.rglob("*.parquet")}):
            name = self._seed_name_for_dir(parquet_dir, seeds_root)
            specs.append(
                SeedSpec(
                    name=name,
                    path=parquet_dir,
                    format="parquet",
                    schema_overrides=schema_overrides.get(name, {}),
                )
            )
        # CSV seeds: any *.csv file (skip if already covered by a parquet dir).
        parquet_names = {s.name for s in specs}
        for csv_file in sorted(seeds_root.rglob("*.csv")):
            name = csv_file.stem
            if name in parquet_names:
                continue
            specs.append(
                SeedSpec(
                    name=name,
                    path=csv_file,
                    format="csv",
                    schema_overrides=schema_overrides.get(name, {}),
                )
            )
        return specs

    @staticmethod
    def _seed_name_for_dir(parquet_dir: Path, seeds_root: Path) -> str:
        rel = parquet_dir.relative_to(seeds_root)
        # For a layout `seeds/<bucket>/<table>/` the logical name is
        # "<bucket>.<table>" so that migrated Snowflake SQL referring to
        # quoted identifiers like "in.c-db.carts" resolves correctly.
        return ".".join(rel.parts) if rel.parts else parquet_dir.name

    def _discover_custom_tests(self) -> list[CustomTestSpec]:
        tests_root = self.root / self.config.tests_path
        if not tests_root.exists():
            return []
        specs: list[CustomTestSpec] = []
        for sql_file in sorted(tests_root.rglob("*.sql")):
            specs.append(
                CustomTestSpec(
                    name=sql_file.stem,
                    path=sql_file,
                    sql=sql_file.read_text(),
                )
            )
        return specs

    def _discover_models(self) -> list[Model]:
        models_root = self.root / self.config.models_path
        if not models_root.exists():
            raise ProjectError(f"Models directory {models_root} does not exist")

        found: list[Model] = []
        # Register each seed as a phantom SEED model so downstream models can
        # declare ref() dependencies on it. The executor skips seed nodes; the
        # real CSV -> table work happens in load_seeds() before the DAG runs.
        for seed in self.seeds:
            found.append(
                Model(
                    name=seed.name,
                    kind=ModelKind.SEED,
                    materialization=Materialization.TABLE,
                    path=seed.path,
                    description=f"CSV seed from {seed.path.name}",
                )
            )

        seed_names = {s.name for s in self.seeds}
        for sql_file in sorted(models_root.rglob("*.sql")):
            found.append(self._load_sql_model(sql_file, seed_names))
        for py_file in sorted(models_root.rglob("*.py")):
            if py_file.name.startswith("_"):
                continue
            found.extend(self._load_python_models(py_file))
        return found

    def _load_sql_model(self, path: Path, seed_names: set[str]) -> Model:
        sql = path.read_text()
        if self.config.jinja:
            sql = _render_jinja(sql, self.config.vars)
        name = path.stem
        refs = extract_refs(sql)
        # Seeds participate in the DAG -- if a model refs a seed via
        # ref(), we treat the seed like a model dependency.
        refs = [r for r in refs if r not in seed_names] + [r for r in refs if r in seed_names]

        schema_meta = self.schemas.get(name, {})
        materialization = Materialization(
            schema_meta.get("materialization", self.config.default_materialization.value)
        )
        columns = [
            ColumnSpec(
                name=col["name"],
                description=col.get("description"),
                tests=col.get("tests", []),
                data_type=col.get("data_type"),
            )
            for col in schema_meta.get("columns", [])
        ]

        return Model(
            name=name,
            kind=ModelKind.SQL,
            materialization=materialization,
            path=path,
            sql=sql,
            depends_on=set(refs),
            description=schema_meta.get("description"),
            columns=columns,
            tags=schema_meta.get("tags", []),
            unique_key=schema_meta.get("unique_key"),
            partition_by=schema_meta.get("partition_by"),
            cluster_by=schema_meta.get("cluster_by"),
            schedule_cron=schema_meta.get("schedule_cron"),
            config=schema_meta.get("config", {}),
        )

    def _load_python_models(self, path: Path) -> list[Model]:
        module_name = f"_juncture_py_models.{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ProjectError(f"Cannot import Python model file {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        models: list[Model] = []
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if not callable(obj) or not is_transform(obj):
                continue
            meta = get_metadata(obj)
            name = meta["name"]
            schema_meta = self.schemas.get(name, {})
            models.append(
                Model(
                    name=name,
                    kind=ModelKind.PYTHON,
                    materialization=meta["materialization"],
                    path=path,
                    python_callable=obj,
                    depends_on=set(meta["depends_on"]),
                    description=meta["description"] or schema_meta.get("description"),
                    columns=[
                        ColumnSpec(
                            name=col["name"],
                            description=col.get("description"),
                            tests=col.get("tests", []),
                            data_type=col.get("data_type"),
                        )
                        for col in (schema_meta.get("columns", []) or meta["columns"])
                    ],
                    tags=meta["tags"] or schema_meta.get("tags", []),
                    unique_key=meta["unique_key"] or schema_meta.get("unique_key"),
                    schedule_cron=meta["schedule_cron"] or schema_meta.get("schedule_cron"),
                    config=meta["config"] | schema_meta.get("config", {}),
                )
            )
        return models


def _load_dotenv_if_present(root: Path) -> None:
    """Populate os.environ from a .env file if one sits next to juncture.yaml."""
    dotenv = root / ".env"
    if not dotenv.exists():
        return
    try:
        from dotenv import load_dotenv
    except ImportError:  # pragma: no cover
        return
    load_dotenv(dotenv, override=False)


def _render_jinja(sql: str, variables: dict[str, Any]) -> str:
    """Render SQL through Jinja with ``ref()`` + ``var()`` helpers.

    ``ref('x')`` stays unchanged so our regex can pick it up downstream;
    this function exists so users can use other Jinja constructs (loops,
    conditionals, variable interpolation) for dbt compatibility.
    """
    import jinja2

    def _ref(name: str) -> str:
        return f"{{{{ ref('{name}') }}}}"

    def _var(key: str, default: Any = None) -> Any:
        return variables.get(key, default)

    env = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )
    template = env.from_string(sql)
    return template.render(ref=_ref, var=_var, **variables)
