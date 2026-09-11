"""Build data/warehouse.duckdb and export the modelling table to Parquet.

Every .sql file in pipeline/sql/ becomes a view named after the file. A file
can select from another by name (bars.sql reads raw_candles), so the views are
created in dependency order.

The warehouse holds queries, not data: every view reads the JSONL on disk at
the moment it is queried. The one thing materialised is the `bars` view,
exported to data/warehouse/bars/symbol=<S>/ as Parquet, which is what
notebooks/data.py loads.

The SQL uses paths relative to the repo root, and DuckDB resolves them when a
view is queried, not when it is created. Open the warehouse from the root:

    duckdb data/warehouse.duckdb

From anywhere else the views fail with "No files found that match the
pattern". The file also takes a single-writer lock, so close any CLI session
on it before re-running this; the error names the process holding it.
"""

import re
from graphlib import TopologicalSorter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "pipeline" / "sql"
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
BARS_DIR = ROOT / "data" / "warehouse" / "bars"


def load_models() -> dict[str, str]:
    """Model name -> SELECT body, with the trailing semicolon removed."""
    return {
        f.stem: f.read_text(encoding="utf-8").strip().rstrip(";")
        for f in sorted(SQL_DIR.glob("*.sql"))
    }


def build_order(models: dict[str, str]) -> list[str]:
    # A view binds when it is created, so anything it selects from has to exist
    # first. A dependency is another model's name appearing as a whole word once
    # comments and string literals are stripped -- a path like 'data/.../bars'
    # must not count as selecting from bars. Enough for SQL this small.
    graph = {}
    for name, body in models.items():
        code = re.sub(r"--[^\n]*", "", body)
        code = re.sub(r"'[^']*'", "''", code)
        graph[name] = {m for m in models if m != name and re.search(rf"\b{m}\b", code)}
    return list(TopologicalSorter(graph).static_order())


def main() -> None:
    # COPY creates its target directory but not missing parents above it.
    BARS_DIR.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE))

    # Creating a view binds it, which reads the files to infer a schema, so
    # the relative paths have to resolve here too, wherever this is run from.
    con.sql(f"SET file_search_path = '{ROOT.as_posix()}'")

    models = load_models()
    for name in build_order(models):
        con.sql(f"CREATE OR REPLACE VIEW {name} AS\n{models[name]}")
        print(f"view {name:<12} <- pipeline/sql/{name}.sql")

    # OVERWRITE replaces the directory instead of writing a second file beside
    # the first, so a rebuild can never double-count a partition.
    con.sql(f"""
        COPY (FROM bars ORDER BY symbol, time)
        TO '{BARS_DIR.as_posix()}' (FORMAT PARQUET, PARTITION_BY (symbol), OVERWRITE)
    """)
    rows = con.sql(f"SELECT count(*) FROM read_parquet('{BARS_DIR.as_posix()}/**/*.parquet')").fetchone()[0]
    print(f"bars -> {BARS_DIR.relative_to(ROOT).as_posix()}/  ({rows} rows)")

    con.close()


if __name__ == "__main__":
    main()
