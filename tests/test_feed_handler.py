"""Controlled load tests for the C++ feed handler, run as a black box.

Each test replays synthetic Coinbase ticker frames through the real binary and
reads back what it wrote. Nothing here touches the network or data/raw/: every
run gets its own --out under pytest's tmp_path.

The zero-loss claims only mean something next to a control, so the block-policy
test has a drop-policy twin under the same load. If the drop run sheds frames
and the block run does not, the pressure was real and backpressure held.
"""

import json
import os
import random
import re
import signal
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EXE = ROOT / "services" / "feed-handler" / "build" / (
    "feed_handler.exe" if os.name == "nt" else "feed_handler")

pytestmark = pytest.mark.skipif(not EXE.exists(), reason="build services/feed-handler first")

SUMMARY = re.compile(
    r"^\s+(frames read|ticks parsed|ticks written|unparseable|dropped|out-of-order)\s*:\s*(\d+)",
    re.MULTILINE)
PACE = re.compile(r"replay pace\s*:\s*([\d.]+)x")


def make_frames(path: Path, n: int, span_s: float = 300.0, seed: int = 7) -> Path:
    """n ticker frames spread evenly over span_s of exchange time.

    Starts with the subscriptions ack a live session opens with, so every run
    also proves non-ticker frames are counted as read but never become ticks.
    Sequence steps by 3 because the real ticker channel skips numbers too.
    """
    rng = random.Random(seed)
    t0 = datetime(2026, 9, 7, 7, 0, tzinfo=timezone.utc)
    price = 79_000.0
    with path.open("w", newline="\n") as f:
        f.write('{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["BTC-USD"]}]}\n')
        for i in range(n):
            price = max(1.0, price + rng.gauss(0, 5))
            t = t0 + timedelta(seconds=span_s * i / max(n - 1, 1))
            frame = {
                "type": "ticker", "sequence": 1_000_000 + 3 * i, "product_id": "BTC-USD",
                "price": f"{price:.2f}", "last_size": f"{rng.uniform(1e-5, 0.5):.8f}",
                "side": rng.choice(["buy", "sell"]),
                "best_bid": f"{price - 0.01:.2f}", "best_bid_size": "0.01",
                "best_ask": f"{price:.2f}", "best_ask_size": "0.02",
                "time": t.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "trade_id": 5_000_000 + i,
            }
            f.write(json.dumps(frame, separators=(",", ":")) + "\n")
    return path


def run(*args: str, timeout: float = 180) -> tuple[int, dict, str]:
    p = subprocess.run([str(EXE), *args], capture_output=True, text=True, timeout=timeout)
    return p.returncode, {k: int(v) for k, v in SUMMARY.findall(p.stderr)}, p.stderr


def written_ticks(out: Path) -> list[dict]:
    return [json.loads(line) for f in sorted(out.rglob("ticks.jsonl"))
            for line in f.read_text().splitlines() if line]


def without_clock(ticks: list[dict]) -> list[dict]:
    # recv_time is stamped at replay time, so it differs run to run by design.
    return [{k: v for k, v in t.items() if k != "recv_time"} for t in ticks]


@pytest.fixture(scope="module")
def frames_20k(tmp_path_factory):
    return make_frames(tmp_path_factory.mktemp("frames") / "f20k.jsonl", 20_000)


@pytest.fixture(scope="module")
def frames_200k(tmp_path_factory):
    return make_frames(tmp_path_factory.mktemp("frames") / "f200k.jsonl", 200_000)


def test_replay_writes_every_frame_in_order(frames_20k, tmp_path):
    code, s, err = run("--replay", str(frames_20k), "--out", str(tmp_path))
    assert code == 0, err
    assert s["frames read"] == 20_001          # 20k ticks + the subscriptions ack
    assert s["ticks parsed"] == s["ticks written"] == 20_000
    assert s["dropped"] == s["unparseable"] == s["out-of-order"] == 0

    ticks = written_ticks(tmp_path)
    assert len(ticks) == 20_000
    assert len({t["trade_id"] for t in ticks}) == 20_000
    seqs = [t["sequence"] for t in ticks]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_backpressure_block_loses_nothing(frames_200k, tmp_path):
    # A 16-slot queue against an unpaced 200k-frame replay: the source outruns
    # the parsers constantly, so this is almost entirely backpressure.
    code, s, err = run("--replay", str(frames_200k), "--out", str(tmp_path),
                       "--capacity", "16", "--parsers", "4", "--overflow", "block")
    assert code == 0, err
    assert s["ticks written"] == 200_000
    assert s["dropped"] == 0
    assert len(written_ticks(tmp_path)) == 200_000


def test_drop_policy_sheds_under_the_same_load(frames_200k, tmp_path):
    # The control. Same load, drop policy: frames must actually be shed, and
    # every frame read must still be accounted for as written or dropped.
    code, s, err = run("--replay", str(frames_200k), "--out", str(tmp_path),
                       "--capacity", "16", "--parsers", "1", "--overflow", "drop")
    assert code == 0, err
    assert s["dropped"] > 0, "no pressure: the block test above proves nothing"
    assert s["ticks written"] + s["dropped"] == 200_000
    assert len(written_ticks(tmp_path)) == s["ticks written"]


def test_graceful_shutdown_drains_everything_accepted(frames_200k, tmp_path):
    # Paced so the run would take ~10 s, then interrupted at 1.5 s. Whatever the
    # source had already read must come out the other end, and the file must
    # end on a complete record.
    kwargs = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {}
    p = subprocess.Popen([str(EXE), "--replay", str(frames_200k), "--out", str(tmp_path),
                          "--speed", "20000"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, **kwargs)
    time.sleep(1.5)
    os.kill(p.pid, signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
    _, err = p.communicate(timeout=60)
    s = {k: int(v) for k, v in SUMMARY.findall(err)}

    assert p.returncode == 0, err
    assert 0 < s["frames read"] < 200_001, "stopped too early or too late to test anything"
    assert s["ticks written"] == s["ticks parsed"] == s["frames read"] - 1   # minus the ack
    assert s["dropped"] == 0
    assert len(written_ticks(tmp_path)) == s["ticks written"]
    for f in tmp_path.rglob("ticks.jsonl"):
        assert f.read_bytes().endswith(b"\n"), f"{f} ends mid-record"


def test_queue_and_pool_choice_do_not_change_output(frames_20k, tmp_path):
    runs = {
        "mutex-1": ("--queue", "mutex", "--parsers", "1"),
        "mutex-4": ("--queue", "mutex", "--parsers", "4"),
        "spsc-1": ("--queue", "spsc", "--parsers", "1"),
    }
    outputs = {}
    for name, flags in runs.items():
        out = tmp_path / name
        code, _, err = run("--replay", str(frames_20k), "--out", str(out), *flags)
        assert code == 0, err
        outputs[name] = without_clock(written_ticks(out))
    assert outputs["mutex-1"] == outputs["mutex-4"] == outputs["spsc-1"]


def test_pace_reaches_100x_real_time(tmp_path):
    frames = make_frames(tmp_path / "paced.jsonl", 3_000, span_s=300.0)
    code, _, err = run("--replay", str(frames), "--out", str(tmp_path / "out"), "--pace", "100")
    assert code == 0, err
    achieved = float(PACE.search(err).group(1))
    # 300 s of market in ~3 s. The ceiling allows for the Windows timer: a
    # frame can be late, never early, so the pace can only come in under.
    assert 95.0 <= achieved <= 100.5, err
