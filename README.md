# Flight Test Analyzer

IADS telemetry analysis pipeline and browser-based root cause analyzer for Cessna 208 AFCS flight test.

## Overview

```
pipeline.sh
  └─ download (iads_export_manual_multiple_download.sh)   day-by-day
  └─ run_batch.py --organize
       └─ analyze_iads.py  (per sortie ZIP → analysis_SXXX.json)
            └─ root_cause_analyzer.html  (load JSONs in browser)
```

---

## Files

| File | Purpose |
|------|---------|
| `analyze_iads.py` | Parallel CSV/ZIP processor — produces one `analysis_SXXX.json` per sortie |
| `run_batch.py` | Batch runner — organizes ZIPs into sortie dirs, runs analyze, deletes ZIPs |
| `pipeline.sh` | Day-by-day download → organize → analyze loop |
| `batch_config.json` | Shared config for all three scripts |
| `root_cause_analyzer.html` | Standalone browser tool for fault analysis |

---

## Quick Start

### 1. Configure

Edit `batch_config.json`:

```json
{
  "data_root": ".",
  "download_script": "~/Documents/GitHub/iads-export/scripts/iads_export_manual_multiple_download.sh",
  "download_start_date": "2026-04-01",
  "download_end_date":   "2026-04-14",
  "download_pattern":    "*",

  "trigger":            "afcsCapable",
  "trigger_from":       1.0,
  "trigger_to":         0.0,

  "workers":            22,
  "parallel_sorties":   3,

  "skip_existing":      true,
  "delete_zips_after":  false
}
```

### 2. Run the pipeline

```bash
bash pipeline.sh
```

Each day is processed independently: download → organize into sortie subdirs → analyze.
Only one day of ZIPs is on disk at a time. The pipeline skips the download step if ZIPs
are already present on disk. Ctrl+C prompts for confirmation before aborting.

### 3. Re-run / force re-analysis

```bash
# In batch_config.json:
#   "skip_existing": false
#   "delete_zips_after": false   (keep ZIPs for retry)

bash pipeline.sh
```

### 4. Analyze without downloading

If ZIPs are already organized into sortie subdirs:

```bash
python run_batch.py
```

If ZIPs are flat in `data_root` (not yet sorted):

```bash
python run_batch.py --organize
```

### 5. Check progress

```bash
python run_batch.py --status
```

Shows a compact status view across all sorties — done, pending, and 0-episode sorties.
A progress bar is also printed inline after each sortie completes during a run.

### 6. Dry-run preview

```bash
bash pipeline.sh --dry-run
python run_batch.py --dry-run
```

---

## batch_config.json

| Key | Default | Description |
|-----|---------|-------------|
| `data_root` | `"."` | Root directory for sortie subdirs |
| `download_script` | — | Path to IADS download script |
| `download_start_date` | — | First day to download (YYYY-MM-DD) |
| `download_end_date` | — | Last day to download (YYYY-MM-DD) |
| `download_pattern` | `"*"` | Filename pattern filter for downloads |
| `trigger` | `"afcsCapable"` | Signal name to detect episodes |
| `trigger_from` | `1.0` | Trigger transition from value |
| `trigger_to` | `0.0` | Trigger transition to value |
| `workers` | `22` | Total worker processes (divided across parallel sorties) |
| `parallel_sorties` | `1` | Number of sorties to analyze concurrently |
| `skip_existing` | `true` | Skip sorties that already have a JSON |
| `delete_zips_after` | `false` | Delete ZIPs after a sortie is analyzed |
| `plot_signals` | see below | Comma-separated signals to capture as continuous data |

### Parallel sorties

`parallel_sorties` runs N sorties concurrently. Workers are divided automatically:
`workers_per_sortie = workers // parallel_sorties`.

Start conservative (2–3). Each sortie extracts ZIPs to temp CSVs — with many parallel
sorties on large datasets, temp disk usage can grow quickly.

Override at runtime:
```bash
python run_batch.py --parallel-sorties 2
```

---

## analyze_iads.py

```
python analyze_iads.py <zip_or_dir> [options]

Options:
  --out PATH            Output JSON path (default: analysis.json)
  --trigger SIGNAL      Trigger signal name   (default: afcsCapable)
  --trigger-from VALUE  Trigger from value    (default: 1.0)
  --trigger-to VALUE    Trigger to value      (default: 0.0)
  --workers N           Parallel workers      (default: auto)
  --plot-signals LIST   Comma-separated signals to capture as continuous data
```

### Output JSON structure

```
{
  "filename":          str,
  "generated_at":      ISO timestamp,
  "total_rows":        int,
  "duration_s":        float,
  "bool_channels":     [...],
  "enum_channels":     [...],
  "num_channels":      [...],
  "mode_transitions":  [...],   ← full-flight latActive/vertActive/atActive history
  "trigger":           { signal, from, to },
  "episodes": [
    {
      "episode":    int,
      "start_time": float,
      "end_time":   float,
      "duration_s": float,
      "transitions": [...],
      "plots": {
        "radAltVoted":   [[t, v], ...],
        "latDevSel":     [[t, v], ...],
        ...
      }
    }
  ]
}
```

### Plot signals captured

| Signal | Description |
|--------|-------------|
| `radAltVoted` | Radio altitude (ft) |
| `gndSpdVoted` | Ground speed (kt) |
| `casVoted` | Calibrated airspeed (kt) |
| `casTarget` | CAS target / commanded speed (kt) |
| `latDevSel` | Lateral deviation — FCC selected (localizer/nav) |
| `vertDevSel` | Vertical deviation — FCC selected (glideslope/GP) |
| `pitchAngleVoted` | Pitch attitude actual (deg) |
| `rollAngleVoted` | Roll attitude actual (deg) |
| `fgPitchCmd` | Flight guidance pitch command (deg) |
| `fgRollCmd` | Flight guidance roll command (deg) |
| `flapAngleVoted` | Flap position (deg) |
| `magHeadingVoted` | Magnetic heading (deg) |
| `GNSS_Latitude` | GNSS latitude coarse (deg) |
| `GNSS_Latitude_Fine` | GNSS latitude fractional (deg) — sum with coarse for precision |
| `GNSS_Longitude` | GNSS longitude coarse (deg) |
| `GNSS_Longitude_Fine` | GNSS longitude fractional (deg) — sum with coarse for precision |

> Only the first matching column is captured per signal (FCC1A preferred over FCC1B).
> Mode enum signals (`latActiveEnum`, `vertActiveEnum`, `atActiveEnum`) are captured for
> both FCC1A and FCC1B and stored in `mode_transitions`.

---

## root_cause_analyzer.html

Open directly in a browser — no server required.

### Loading data

- **Drop a JSON** onto the drop zone, or click to browse
- **Folder scan** — picks up all `analysis*.json` files from a directory and subdirectories
- Multiple sorties queue in the sidebar; click **Data Summary** for a fleet-level view

### Fault classification

Episodes (AFCS capable 1→0 transitions) are classified by `classifyExit()`:

| Category | Description |
|----------|-------------|
| `PILOT_CMD` | AP disconnect before any monitor flag |
| `RESP_MONITOR` | Response monitor gate deactivated |
| `CMD_MONITOR` | Command monitor gate deactivated |
| `MONITOR_FAULT` | Generic monitor flag asserted |
| `MISTRIM` | Mistrim / XCD threshold exceeded |
| `VALIDITY_LOSS` | Sensor/data validity lost pre-trigger |
| `CAP_LOST` | Upstream capability lost (cascade) |
| `UNKNOWN` | No matching pattern found |

> Torque Limiting is excluded from fault classification — it is a protection mechanism, not a fault.

### Flight phases

When `mode_transitions` is present in the JSON, the tool automatically detects:

- **APPROACH** — `latActive = navAppr` and/or `vertActive = glidePath`
- **LANDING** — `vertActive = flare`, `latActive = align`, or `atActive = retard`

Each phase card shows:
- Entry/exit AGL, entry CAS, CAS target, ground speed, flap, heading
- Lateral and vertical deviation — max absolute, RMS, sparkline chart
- Pitch and roll tracking — actual vs commanded (solid vs dashed), max/RMS error
- Touchdown lat/lon with Google Maps link (landing phase only)
- Mode sequence timeline (FCC1A and FCC1B)
- Any AFCS faults that occurred during the phase

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Exclude signals | `apQuickDisconnect, togaPb, apPb` | Episodes containing these are treated as pilot-commanded and hidden |
| Max transitions per episode | 50 | Limits concurrent signal list length |

### Printing

- **Print Detail** — full episode cards for the active sortie (light theme)
- **Print Summary** — Data Summary view across all loaded sorties (light theme)

---

## Pipeline output

`pipeline.sh` reports download time, analysis time, and total elapsed per day and full run.
The download step is automatically skipped if ZIPs are already on disk.

```
════════════════════════════════════════════════════════
  Day 1 / 14  —  2026-04-01
════════════════════════════════════════════════════════

  [1/2] Skipping download — 24 ZIP(s) already on disk
  Download: 0s

  [2/2] Organize + Analyze...
Batch  .  |  8 sortie(s)  |  trigger=afcsCapable  |  parallel=3  workers/sortie=7
[1/8]  S114_1  (8 ZIP(s))
[2/8]  S115_1  (8 ZIP(s))
[3/8]  S115_2  (8 ZIP(s))
  [########----------------------] 3/8 (37%)  pending=5
  ...
Batch done  312.4s  |  ok=8  skipped=0  errors=0
```

---

## Temp disk space

Each ZIP is extracted to a temp CSV before parallel analysis, then deleted on completion.
With `parallel_sorties=3` and large ZIPs, peak temp usage can reach 20–40 GB.

- Clean up orphaned temp files after a killed run: `rm $TEMP/_rca_tmp_*.csv`
- Set `delete_zips_after: true` to free source ZIPs as each sortie finishes
- Reduce `parallel_sorties` if disk space is limited

---

## Signal naming reference (Murray flight software)

| Suffix | Full path example |
|--------|-------------------|
| `latActive` | `FCC1A.g_fglatcontrollaw_mdlrefdw.rtb.latActiveEnum` |
| `vertActive` | `FCC1A.g_fgaltcontrollaw_mdlrefdw.rtb.vertActiveEnum` |
| `latDevSel` | `FCC1A.g_sensorfilters_mdlrefdw.rtb.latDevSel` |
| `vertDevSel` | `FCC1A.g_sensorfilters_mdlrefdw.rtb.vertDevSel` |
| `GNSS_Latitude` | `FMS1.FMS1_A743.110.GNSS_Latitude` |
| `GNSS_Latitude_Fine` | `FMS1.FMS1_A743.120.GNSS_Latitude_Fine` |
