"""Compact closed hours of raw ticks into the Parquet lake, gated on quality.

    data/raw/ticks/symbol=S/dt=D/hour=H/ticks.jsonl      landing zone (C++ appends)
    data/lake/ticks/symbol=S/dt=D/hour=H/ticks.parquet   lake (this script writes)

The feed handler writes JSONL because a line is the unit a crash can damage and
the writer can heal; Parquet cannot be appended to at all. So ticks land as
JSONL and each hour is published to the lake once it is closed -- its end at
least five minutes past, so the writer has moved on. The JSONL stays: it is the
source of truth, and tick_bars still reads it.

Before publishing, every hour runs through quality.check_ticks. An hour with an
error-level finding (schema, uniqueness, range) is held back and reported, not
written; anomalies are reported and published. Types are cast to the contract
explicitly rather than trusting inference, and each file is written beside its
destination and renamed into place, so a reader never sees half a partition.

Idempotent: an hour whose Parquet is newer than its JSONL is skipped. Run it as
often as you like.

    uv run python pipeline/compact_ticks.py
"""

import datetime as dt
import os
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quality as q  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "ticks"
LAKE = ROOT / "data" / "lake" / "ticks"
GRACE = dt.timedelta(minutes=5)


def partition_end(jsonl: Path) -> dt.datetime:
    """End of the hour a raw partition covers, from its dt=/hour= folder names."""
    hour_dir, day_dir = jsonl.parent, jsonl.parent.parent
    start = dt.datetime.strptime(f"{day_dir.name[3:]} {hour_dir.name[5:]}", "%Y-%m-%d %H")
    return start.replace(tzinfo=dt.timezone.utc) + dt.timedelta(hours=1)


def compact(jsonl: Path, now: dt.datetime) -> str:
    rel = jsonl.parent.relative_to(RAW)
    out = LAKE / rel / "ticks.parquet"

    if partition_end(jsonl) + GRACE > now:
        return f"open     {rel.as_posix()}"
    if out.exists() and out.stat().st_mtime >= jsonl.stat().st_mtime:
        return f"current  {rel.as_posix()}"

    con = duckdb.connect()
    df = con.sql(f"SELECT * FROM read_json_auto('{jsonl.as_posix()}', hive_partitioning = false)").df()
    findings = q.check_ticks(df)
    if any(f.is_error for f in findings):
        return f"HELD     {rel.as_posix()}  ({len(df)} ticks)\n{q.summarize(findings)}"

    cols = ", ".join(f"CAST({c} AS {t}) AS {c}" for c, t in q.TICK_SCHEMA.items())
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    con.register("df", df)
    con.sql(f"COPY (SELECT {cols} FROM df ORDER BY sequence) TO '{tmp.as_posix()}' (FORMAT PARQUET)")
    written = con.sql(f"SELECT count(*) FROM read_parquet('{tmp.as_posix()}')").fetchone()[0]
    if written != len(df):
        tmp.unlink()
        raise RuntimeError(f"{rel}: wrote {written} rows, read {len(df)}")
    os.replace(tmp, out)

    line = f"wrote    {rel.as_posix()}  ({written} ticks)"
    return line if not findings else f"{line}\n{q.summarize(findings)}"


def main() -> None:
    now = dt.datetime.now(dt.timezone.utc)
    partitions = sorted(RAW.glob("symbol=*/dt=*/hour=*/ticks.jsonl"))
    if not partitions:
        print(f"no raw ticks under {RAW.relative_to(ROOT).as_posix()}/")
    for jsonl in partitions:
        print(compact(jsonl, now))


if __name__ == "__main__":
    main()
