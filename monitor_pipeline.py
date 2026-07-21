#!/usr/bin/env python3
"""
monitor_pipeline.py — live terminal dashboard for pipeline_parallel.sh chunks.

Usage:
    python monitor_pipeline.py                     # auto-detect config + chunks
    python monitor_pipeline.py batch_config.json   # explicit config
    python monitor_pipeline.py --chunks=8          # override chunk count
    python monitor_pipeline.py --log-dir=/tmp      # log file location
    python monitor_pipeline.py --interval=10       # refresh every N seconds (default 10)
    python monitor_pipeline.py --once              # print once and exit

Reads:
  - /tmp/pipeline_chunk_N.log  (one per chunk)
  - data_root/.stage/chunk_N/.pipeline_done/  (markers)
  - data_root/.stage/chunk_N/  (sortie dirs)
"""

import argparse
import datetime
import io
import json
import os
import pathlib
import re
import sys
import tempfile
import time

# Force UTF-8 stdout so block/box chars work on Windows terminals
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── ANSI helpers ──────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
RED    = "\033[31m"
WHITE  = "\033[37m"
BLUE   = "\033[34m"
BG_DARK = "\033[48;5;234m"

def clr(text, *codes):
    return "".join(codes) + text + RESET

def cursor_home():
    sys.stdout.write("\033[H")

def clear_screen():
    sys.stdout.write("\033[2J\033[H")

def hide_cursor():
    sys.stdout.write("\033[?25l")

def show_cursor():
    sys.stdout.write("\033[?25h")

# ── Log parsing ───────────────────────────────────────────────────────────────

RE_DATES  = re.compile(r"dates\s*:\s*(\d{4}-\d{2}-\d{2})\s*->\s*(\d{4}-\d{2}-\d{2})\s*\((\d+)\s*day")
RE_DAY    = re.compile(r"Day\s+(\d+)\s*/\s*(\d+)\s*.{1,6}(\d{4}-\d{2}-\d{2})")
RE_DONE   = re.compile(r"Pipeline complete|pipeline complete|All chunks finished")
RE_SORTIE = re.compile(r"\[(\d+)/(\d+)\]\s+(\S+)\s+\(")


def parse_log(log_path: str) -> dict:
    info = {
        "exists": False,
        "start_date": None,
        "end_date": None,
        "total_days": 0,
        "current_day_num": 0,
        "current_date": None,
        "done": False,
        "mtime": 0,
        "last_line": "",
        "started_at": None,
    }
    try:
        with open(log_path, "r", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return info

    info["exists"] = True
    info["mtime"] = os.path.getmtime(log_path)

    for line in lines:
        m = RE_DATES.search(line)
        if m:
            info["start_date"] = datetime.date.fromisoformat(m.group(1))
            info["end_date"]   = datetime.date.fromisoformat(m.group(2))
            info["total_days"] = int(m.group(3))
        m = RE_DAY.search(line)
        if m:
            info["current_day_num"] = int(m.group(1))
            info["current_date"]    = datetime.date.fromisoformat(m.group(3))
        if "started" in line.lower():
            ts_m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if ts_m:
                info["started_at"] = datetime.datetime.fromisoformat(ts_m.group(1))
        if RE_DONE.search(line):
            info["done"] = True

    for line in reversed(lines):
        stripped = line.strip()
        if stripped:
            info["last_line"] = stripped[:80]
            break

    return info


def count_markers_in_range(
    marker_dir: str, start: datetime.date, end: datetime.date
) -> tuple[int, datetime.date | None]:
    """Returns (count, latest_date_in_range)."""
    if not os.path.isdir(marker_dir):
        return 0, None
    count = 0
    latest = None
    for name in os.listdir(marker_dir):
        try:
            d = datetime.date.fromisoformat(name)
            if start <= d <= end:
                count += 1
                if latest is None or d > latest:
                    latest = d
        except ValueError:
            pass
    return count, latest


def count_sortie_dirs(stage_dir: str) -> int:
    if not os.path.isdir(stage_dir):
        return 0
    return sum(
        1 for e in os.scandir(stage_dir)
        if e.is_dir() and not e.name.startswith(".")
    )


# ── Progress bar ──────────────────────────────────────────────────────────────

def bar(done: int, total: int, width: int = 20) -> str:
    if total == 0:
        return "[" + " " * width + "]"
    filled = int(round(done / total * width))
    return "[" + "█" * filled + "░" * (width - filled) + "]"


# ── Status detection ──────────────────────────────────────────────────────────

STALE_SECS = 180   # no log update in 3 min = stalled


def chunk_status(info: dict, markers_done: int) -> tuple[str, str]:
    """Returns (label, color_code)."""
    if not info["exists"]:
        return "NO LOG", DIM
    if info["done"] or (info["total_days"] > 0 and markers_done >= info["total_days"]):
        return "DONE   ", GREEN
    age = time.time() - info["mtime"]
    if age > STALE_SECS:
        return f"STALLED", YELLOW
    return "running", CYAN


# ── Main render ───────────────────────────────────────────────────────────────

def render(chunks: list[dict], data_root: str, refresh_interval: int):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Totals
    total_days_all = sum(c["log"]["total_days"] for c in chunks)
    total_markers  = sum(c["markers"] for c in chunks)
    total_sorties  = sum(c["sorties"] for c in chunks)

    lines = []
    W = 100

    def hdr(text):
        return clr(f"  {text}", BOLD, CYAN)

    lines.append(clr("═" * W, CYAN))
    lines.append(clr(
        f"  Pipeline Monitor — {data_root}   "
        f"chunks={len(chunks)}   refreshed {now}",
        BOLD, WHITE
    ))
    lines.append(clr("═" * W, CYAN))
    lines.append("")

    # Column header
    lines.append(
        clr("  Ch  ", BOLD) +
        clr("Date Range             ", BOLD) +
        clr("Progress     ", BOLD) +
        clr("Current Date  ", BOLD) +
        clr("Markers  ", BOLD) +
        clr("Sorties  ", BOLD) +
        clr("Status   ", BOLD) +
        clr("Last activity", BOLD)
    )
    lines.append(clr("  " + "─" * (W - 2), DIM))

    for c in chunks:
        log   = c["log"]
        idx   = c["idx"]
        mk    = c["markers"]
        total = log["total_days"]
        cur_d = c["latest_date"].isoformat() if c["latest_date"] else "—"
        dr    = (
            f"{log['start_date'].strftime('%m-%d')}→{log['end_date'].strftime('%m-%d')}"
            if log["start_date"] else "—"
        )

        status_label, status_color = chunk_status(log, mk)

        # Progress bar (compact)
        pct = mk / total if total else 0
        pbar = bar(mk, total, 12)
        prog_str = f"{pbar} {mk:3}/{total:3}"

        last = log["last_line"]
        if len(last) > 32:
            last = last[:29] + "..."

        line = (
            clr(f"  {idx:2}  ", BOLD) +
            f"{dr:<23}" +
            f"{prog_str:<25}" +
            f"{cur_d:<14}" +
            f"{mk:^9}" +
            f"{c['sorties']:^9}" +
            clr(f"{status_label:<9}", status_color) +
            clr(last, DIM)
        )
        lines.append(line)

    lines.append(clr("  " + "─" * (W - 2), DIM))

    # Totals row
    total_pct = total_markers / total_days_all * 100 if total_days_all else 0
    lines.append(
        clr("  ──  ", BOLD) +
        clr(f"{'TOTAL':<23}", BOLD) +
        clr(f"{bar(total_markers, total_days_all, 12)} {total_markers:3}/{total_days_all:3}", BOLD) +
        " " * 14 +
        clr(f"{total_markers:^9}", BOLD) +
        clr(f"{total_sorties:^9}", BOLD) +
        clr(f"{total_pct:5.1f}%", GREEN if total_pct == 100 else YELLOW)
    )
    lines.append("")

    # Elapsed / ETA
    started_times = [c["log"]["started_at"] for c in chunks if c["log"]["started_at"]]
    if started_times:
        earliest = min(started_times)
        elapsed  = datetime.datetime.now() - earliest
        el_str   = str(elapsed).split(".")[0]
        if total_pct > 1:
            eta_secs = elapsed.total_seconds() / (total_pct / 100) * (1 - total_pct / 100)
            eta = str(datetime.timedelta(seconds=int(eta_secs)))
        else:
            eta = "calculating..."
        lines.append(clr(f"  Elapsed: {el_str}   ETA: {eta}   "
                         f"Next refresh in {refresh_interval}s  (Ctrl+C to exit)", DIM))
    lines.append(clr("═" * W, CYAN))

    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

def resolve_data_root(config_path: str) -> str:
    script_dir = os.path.dirname(os.path.abspath(config_path))
    try:
        with open(config_path) as f:
            cfg = json.load(f)
        dr = cfg.get("data_root", ".")
        if not os.path.isabs(dr):
            dr = os.path.join(script_dir, dr)
        return os.path.normpath(dr)
    except Exception:
        return script_dir


def main():
    p = argparse.ArgumentParser(description="Pipeline chunk monitor")
    p.add_argument("config", nargs="?", default="batch_config.json")
    p.add_argument("--chunks", type=int, default=None)
    p.add_argument("--log-dir", default=tempfile.gettempdir())
    p.add_argument("--interval", type=int, default=10)
    p.add_argument("--once", action="store_true")
    args = p.parse_args()

    config_path = args.config
    if not os.path.isabs(config_path):
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), config_path)

    data_root = resolve_data_root(config_path)

    # Auto-detect chunk count from log files present
    if args.chunks is None:
        detected = 0
        for i in range(1, 33):
            if os.path.exists(os.path.join(args.log_dir, f"pipeline_chunk_{i}.log")):
                detected = i
        n_chunks = detected if detected > 0 else 8
    else:
        n_chunks = args.chunks

    if args.once:
        hide_cursor()
    else:
        clear_screen()
        hide_cursor()

    try:
        while True:
            chunks = []
            for i in range(1, n_chunks + 1):
                log_path   = os.path.join(args.log_dir, f"pipeline_chunk_{i}.log")
                stage_dir  = os.path.join(data_root, ".stage", f"chunk_{i}")
                marker_dir = os.path.join(stage_dir, ".pipeline_done")
                log              = parse_log(log_path)
                markers, latest  = (
                    count_markers_in_range(marker_dir, log["start_date"], log["end_date"])
                    if log["start_date"] else (0, None)
                )
                sorties = count_sortie_dirs(stage_dir)
                chunks.append({
                    "idx": i, "log": log,
                    "markers": markers, "latest_date": latest,
                    "sorties": sorties,
                })

            output = render(chunks, os.path.relpath(data_root), args.interval)

            if args.once:
                print(output)
                break

            cursor_home()
            sys.stdout.write(output)
            sys.stdout.write("\033[J")   # clear from cursor to end of screen
            sys.stdout.flush()
            time.sleep(args.interval)

    except KeyboardInterrupt:
        pass
    finally:
        show_cursor()
        if not args.once:
            print()


if __name__ == "__main__":
    main()
