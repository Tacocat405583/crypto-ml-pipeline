"""Build data/warehouse.duckdb: one view per .sql file in pipeline/sql/.

The warehouse holds queries, not data. Every view reads the JSONL on disk at
the moment it is queried, so ticks the feed handler writes after this runs
show up in tick_bars without a rebuild. Re-run only when a .sql file changes.

The SQL uses paths relative to the repo root, and DuckDB resolves them when a
view is queried, not when it is created. Open the warehouse from the root:

    duckdb data/warehouse.duckdb

From anywhere else the views fail with "No files found that match the
pattern". The file also takes a single-writer lock, so close any CLI session
on it before re-running this; the error names the process holding it.
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "pipeline" / "sql"
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"


def main() -> None:
    WAREHOUSE.parent.mkdir(exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE))

    # Creating a view binds it, which reads the files to infer a schema, so
    # the relative paths have to resolve here too, wherever this is run from.
    con.sql(f"SET file_search_path = '{ROOT.as_posix()}'")

    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        body = sql_file.read_text(encoding="utf-8").strip().rstrip(";")
        con.sql(f"CREATE OR REPLACE VIEW {sql_file.stem} AS\n{body}")
        print(f"view {sql_file.stem:<12} <- {sql_file.relative_to(ROOT).as_posix()}")

    con.close()


if __name__ == "__main__":
    main()
