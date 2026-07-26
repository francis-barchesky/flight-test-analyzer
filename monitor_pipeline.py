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

RE_DATES        = re.compile(r"dates\s*:\s*(\d{4}-\d{2}-\d{2})\s*->\s*(\d{4}-\d{2}-\d{2})\s*\((\d+)\s*day")
RE_DAY          = re.compile(r"Day\s+(\d+)\s*/\s*(\d+)\s*.{1,6}(\d{4}-\d{2}-\d{2})")
RE_DONE         = re.compile(r"Pipeline complete|pipeline complete|All chunks finished")
RE_SORTIE       = re.compile(r"\[(\d+)/(\d+)\]\s+(\S+)\s+\(")
RE_CHUNK_SORTIES = re.compile(r"#\s*chunk_sorties:\s*(\d+)")
RE_CHUNK_STAGE   = re.compile(r"#\s*chunk_stage_dir:\s*(.+)")


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
        "chunk_sorties": None,
        "stage_dir": None,
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
        m = RE_CHUNK_SORTIES.search(line)
        if m:
            info["chunk_sorties"] = int(m.group(1))
        m = RE_CHUNK_STAGE.search(line)
        if m:
            info["stage_dir"] = m.group(1).strip()
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

STALE_SECS = 600   # no log update in 10 min = stalled (analysis can take ~5-8 min per sortie)


def chunk_status(info: dict, markers_done: int) -> tuple[str, str]:
    """Returns (label, color_code)."""
    if not info["exists"]:
        return "NO LOG ", DIM
    age = time.time() - info["mtime"]
    if info["done"] or (info["total_days"] > 0 and markers_done >= info["total_days"]):
        return "DONE   ", GREEN
    if age > STALE_SECS:
        return "STALLED", YELLOW
    return "running", CYAN


# ── Main render ───────────────────────────────────────────────────────────────

def render(chunks: list[dict], data_root: str, refresh_interval: int):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Totals — use sortie counts when available, fall back to marker counts
    sortie_mode = any(c["log"]["chunk_sorties"] is not None for c in chunks)
    total_assigned = sum(c["log"]["chunk_sorties"] or 0 for c in chunks)
    total_staged   = sum(c["sorties"] for c in chunks if c["sorties"] >= 0)

    lines = []
    W = 90

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
        clr("Assigned      ", BOLD) +
        clr("Progress (staged/assigned)     ", BOLD) +
        clr("Status   ", BOLD) +
        clr("Last activity", BOLD)
    )
    lines.append(clr("  " + "─" * (W - 2), DIM))

    for c in chunks:
        log    = c["log"]
        idx    = c["idx"]
        staged = c["sorties"] if c["sorties"] >= 0 else 0
        assigned = log["chunk_sorties"]

        if assigned is not None:
            assigned_str = f"{assigned} sorties"
            staged_disp = min(staged, assigned)
            pbar = bar(staged_disp, assigned, 16)
            prog_str = f"{pbar} {staged_disp}/{assigned}"
        else:
            # fallback: marker-based progress for old-style logs
            mk    = c["markers"]
            total = log["total_days"]
            dr    = (f"{log['start_date'].strftime('%m-%d')}→{log['end_date'].strftime('%m-%d')}"
                     if log["start_date"] else "—")
            assigned_str = dr
            pbar = bar(mk, total, 16)
            prog_str = f"{pbar} {mk}/{total}"

        status_label, status_color = chunk_status(log, c["markers"])

        last = log["last_line"]
        if len(last) > 36:
            last = last[:33] + "..."

        line = (
            clr(f"  {idx:2}  ", BOLD) +
            f"{assigned_str:<14}" +
            f"{prog_str:<31}" +
            clr(f"{status_label:<9}", status_color) +
            clr(last, DIM)
        )
        lines.append(line)

    lines.append(clr("  " + "─" * (W - 2), DIM))

    # Totals row
    if sortie_mode and total_assigned > 0:
        total_staged_disp = min(total_staged, total_assigned)
        total_pct = min(total_staged_disp / total_assigned * 100, 100.0)
        lines.append(
            clr("  ──  ", BOLD) +
            clr(f"{'TOTAL':<14}", BOLD) +
            clr(f"{bar(total_staged_disp, total_assigned, 16)} {total_staged_disp}/{total_assigned}", BOLD) +
            "          " +
            clr(f"{total_pct:5.1f}%", GREEN if total_pct == 100 else YELLOW)
        )
    else:
        total_days_all = sum(c["log"]["total_days"] for c in chunks)
        total_markers  = sum(c["markers"] for c in chunks)
        total_pct = total_markers / total_days_all * 100 if total_days_all else 0
        lines.append(
            clr("  ──  ", BOLD) +
            clr(f"{'TOTAL':<14}", BOLD) +
            clr(f"{bar(total_markers, total_days_all, 16)} {total_markers}/{total_days_all}", BOLD) +
            "          " +
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
            # Fall back to main data_root markers when no staging dirs exist
            staging_root = os.path.join(data_root, ".stage")
            has_staging  = os.path.isdir(staging_root)
            main_marker_dir = os.path.join(data_root, ".pipeline_done")

            chunks = []
            for i in range(1, n_chunks + 1):
                log_path  = os.path.join(args.log_dir, f"pipeline_chunk_{i}.log")
                log       = parse_log(log_path)

                # Prefer stage_dir from log header (supports data_root_map multi-tail)
                if log["stage_dir"] and os.path.isdir(log["stage_dir"]):
                    stage_dir  = log["stage_dir"]
                    marker_dir = os.path.join(stage_dir, ".pipeline_done")
                    sorties    = count_sortie_dirs(stage_dir)
                elif has_staging:
                    stage_dir = os.path.join(staging_root, f"chunk_{i}")
                    if os.path.isdir(stage_dir):
                        marker_dir = os.path.join(stage_dir, ".pipeline_done")
                        sorties    = count_sortie_dirs(stage_dir)
                    else:
                        marker_dir = main_marker_dir
                        sorties    = -1
                else:
                    # No staging — read from main pipeline_done, sorties N/A
                    marker_dir = main_marker_dir
                    sorties    = -1  # sentinel: display as "—"

                markers, latest = (
                    count_markers_in_range(marker_dir, log["start_date"], log["end_date"])
                    if log["start_date"] else (0, None)
                )
                chunks.append({
                    "idx": i, "log": log,
                    "markers": markers, "latest_date": latest,
                    "sorties": sorties,
                })

            output = render(chunks, os.path.relpath(data_root), args.interval)

            if args.once:
                print(output)
                break

            clear_screen()
            sys.stdout.write(output)
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
