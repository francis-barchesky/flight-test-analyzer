# Flight Test Analyzer

IADS telemetry analysis pipeline and browser-based root cause analyzer for Cessna 208 AFCS flight test.

## Overview

```
pipeline.sh
  └─ download (iads_export_manual_multiple_download.sh)   day-by-day
  └─ run_batch.py --organize
       └─ analyze_iads.py  (per sortie ZIP → analysis_SXXX.json)
            └─ flight_test_analyzer.html  (load JSONs in browser)
```

---

## Files

| File | Purpose |
|------|---------|
| `analyze_iads.py` | Parallel CSV/ZIP processor — produces one `analysis_SXXX.json` per sortie |
| `run_batch.py` | Batch runner — organizes ZIPs into sortie dirs, runs analyze, deletes ZIPs |
| `pipeline.sh` | Day-by-day download → organize → analyze loop |
| `batch_config.json` | Shared config for all three scripts |
| `flight_test_analyzer.html` | Standalone browser tool for fault analysis |
| `correlate_faults.py` | Fleet-wide correlation — ranks signals by how consistently they transition before fault episodes; writes `fault_correlations.json` |
| `classify_episodes.py` | Classifies all AFCS disengagement episodes using `classifyExit()` logic; prints distribution and writes `plots/plot_fault_dist.pdf` |
| `trace_upstream.py` | Walks the model trace graph upstream from a target signal; reports TestPoint signals with hop counts; cross-references `fault_correlations.json` |
| `generate_plots.py` | Generates proposal appendix PDF figures from `fault_correlations.json` and `upstream_afcsCapable.json` |
| `generate_hires.py` | Regenerates `*_hires.json` files without re-running full analysis |
| `patch_plots.py` | Adds new plot signals to existing analysis JSONs without re-analyzing (reads only the relevant ZIP) |
| `patch_sysnotengage.py` | Back-fills `sysNotEngage` from existing transition data into analysis JSONs |
| `patch_signal_from_zip.py` | Patches any signal from a bulk-export ZIP into the matching analysis JSON |
| `patch_all_sorties.py` | Batch patch a missing signal across all sorties (downloads raw IADS data, exports, patches) |
| `patch_torque_stats.py` | Back-fills `torque_stats` key into existing analysis JSONs |

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

Shows a compact status view across all sorties — done, pending, and 0-episode sorties:

```
Status  .  (9 sortie(s) with ZIPs)

  DONE    (5):  G014  S114_1  S115_2  S116  S116_2
  PENDING (4):  S115_1  S117  S117_2  S118
  0 eps   (2):  S114_1  S116_2

  5/9 done (55%)  |  4 pending  |  2 with 0 episodes
```

A live progress bar with ETA is also printed after each sortie completes during a run:

```
  [##########--------------------] 3/9 (33%)  ETA ~14m 22s (15:43)
```

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
| `workers` | `0` | Total worker processes — `0` auto-detects all CPU cores |
| `parallel_sorties` | `1` | Number of sorties to analyze concurrently |
| `skip_existing` | `true` | Skip sorties that already have a JSON |
| `delete_zips_after` | `false` | Delete ZIPs after a sortie is analyzed |
| `plot_signals` | see below | Comma-separated signals to capture as continuous data |

### Worker auto-detection

Set `workers: 0` to automatically use all available CPU cores. The batch header
always shows the resolved count:

```
Batch  .  |  9 sortie(s)  |  trigger=afcsCapable  |  parallel=3  workers/sortie=7 (auto/22)
```

### Parallel sorties

`parallel_sorties` runs N sorties concurrently. Workers are divided automatically:
`workers_per_sortie = workers // parallel_sorties`.

Start conservative (2–3). Each sortie extracts ZIPs to temp CSVs — with many parallel
sorties on large datasets, temp disk usage can grow quickly. If the bottleneck is a single
large ZIP, fewer parallel sorties with more workers each will be faster.

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
  "sysNotEngage":      [...],   ← all sysNotEngage transitions (if signal present in recording)
  "trigger":           { signal, from, to },
  "flight_plots": {             ← full-resolution plot signals for approach/landing window
    "radAltVoted":   [[t, v], ...],
    "latDevSel":     [[t, v], ...],
    ...
  },
  "takeoff_plots": {            ← full-resolution plot signals for takeoff window
    "radAltVoted":   [[t, v], ...],
    "casVoted":      [[t, v], ...],
    ...
  },
  "data_gaps": [                ← recording discontinuities >1 s detected in timestamps
    { "t_start": float, "t_end": float, "duration_s": float },
    ...
  ],
  "episodes": [
    {
      "episode":    int,
      "start_time": float,
      "end_time":   float,
      "duration_s": float,
      "transitions": [...],
      "plots": {                ← ±30s episode window (fallback when flight_plots absent)
        "radAltVoted":   [[t, v], ...],
        ...
      }
    }
  ]
}
```

### flight_plots — approach/landing window at full resolution

`flight_plots` contains all plot-signal samples within the approach/landing time window
at the native IADS recording rate — no downsampling.

**Window**: from the first activation of any approach/landing mode
(`navAppr`, `glidePath`, `align`, `flare`, `retard`) minus 10 s, through the last
activation **or deactivation** of any such mode plus 5 s.

Using the deactivation time (mode going back to standby after touchdown) rather than the
last activation ensures the full post-touchdown ground roll is captured.

Typical size: ~5–10 MB added to the JSON for a 6-minute approach at 50 Hz.

If no approach modes are detected (e.g. ground sorties), all plot-signal points
are stored for the full flight.

### takeoff_plots — takeoff window at full resolution

`takeoff_plots` contains all plot-signal samples within the takeoff time window
at the native IADS recording rate.

**Window**: from the first activation of any takeoff mode
(`latActive=takeoff`, `vertActive=takeoff`, `atActive=takeOff`) minus 10 s, through the
last deactivation plus 30 s.

The +30 s post-margin captures the initial climb-out after the AFCS modes revert to
standby. Stored as an empty dict `{}` when no takeoff modes are found.

The browser tool uses `takeoff_plots` when rendering TAKEOFF phase cards.

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

## Fleet analysis scripts

These scripts run after the pipeline has produced analysis JSONs for all sorties.

### correlate_faults.py

Scans all `analysis_*.json` files and ranks signals by how consistently they transition in the pre-trigger window around each fault episode.

```bash
python correlate_faults.py [data_root] [options]

Options:
  --window-pre  SECS    Seconds before trigger to include (default: 5)
  --window-post SECS    Seconds after  trigger to include (default: 1)
  --min-freq    FREQ    Minimum episode frequency to keep  (default: 0.05)
  --top         N       Max signals in output              (default: 100)
  --sample-rate HZ      Model rate for dt-in-samples output (default: 40)
  --exclude     SIGNAL  Suffix patterns to exclude (supports * wildcards)
```

Writes `fault_correlations.json` to `data_root`. Top 10 signals are printed to stdout.

### classify_episodes.py

Ports the browser's `classifyExit()` logic to Python. Reads all analysis JSONs, classifies every AFCS disengagement episode, prints a category distribution, and writes `plots/plot_fault_dist.pdf`.

```bash
python classify_episodes.py
```

### trace_upstream.py

Walks the model trace graph (BFS) upstream from a target signal and reports all observable (TestPoint) signals with hop counts and model names. Optionally cross-references `fault_correlations.json` to show which upstream signals also appear as correlated fault precursors.

```bash
python trace_upstream.py --signal afcsCapable [data_root]
python trace_upstream.py --signal afcsCapable --trace path/to/traceData.json .
python trace_upstream.py --signal afcsCapable --max-hops 20 .
```

Writes `upstream_<signal>.json` to `data_root`.

### generate_plots.py

Generates four PDF figures for the proposal appendix into `plots/`:

| File | Contents |
|------|----------|
| `plot_correlations.pdf` | Top 20 correlated signals — horizontal bar chart coloured by timing cluster |
| `plot_timing.pdf` | Mean Δt vs score scatter, bubble size proportional to frequency |
| `plot_hop_dist.pdf` | Upstream TestPoint count by hop depth + cumulative line |
| `plot_sortie_times.pdf` | Per-sortie processing times sorted by duration |

Requires `fault_correlations.json` and `upstream_afcsCapable.json` in `data_root`.

```bash
python generate_plots.py
```

### generate_hires.py

Regenerates `*_hires.json` files from the existing analysis JSON and raw ZIPs, without re-running the full analysis pass.

```bash
python generate_hires.py                    # all sorties
python generate_hires.py S115_1_N208B       # one sortie
python generate_hires.py --force            # overwrite existing hires files
```

### patch_plots.py

Adds new plot signals to existing analysis JSONs without re-analyzing. Reads only the ZIP that contains the requested signal.

```bash
python patch_plots.py <signal1,signal2,...> [data_root]
python patch_plots.py azVoted .
python patch_plots.py "azVoted,nzVoted" /path/to/data
python patch_plots.py azVoted --dry-run     # preview without writing
python patch_plots.py azVoted --force       # re-patch even if already present
```

---

## flight_test_analyzer.html

Open directly in a browser — no server required.

### Loading data

- **📂 Load data** (header bar) — quick-access button; same as Scan folder
- **Scan folder** (top of sidebar) — scans a directory and all subdirectories:
  - Loads all `analysis_*.json` files into the file queue
  - Auto-detects and loads any trace graph JSON (file containing `nodes[]` + `edges[]`)
  - Other JSONs (`batch_config.json`, `batch_manifest.json`, etc.) are silently ignored
- **Drop analysis.json** onto the Analysis Data drop zone, or click to browse individual files
- Multiple sorties queue in the sidebar; click **Data Summary** for a fleet-level view

### Fault classification

Episodes (AFCS capable 1→0 transitions) are classified by `classifyExit()`:

| Category | Description |
|----------|-------------|
| `PILOT_CMD` | AP disconnect / TOGA / apQuickDisconnect before any monitor flag |
| `RESP_MONITOR` | Response monitor gate deactivated |
| `CMD_MONITOR` | Command monitor gate deactivated |
| `MONITOR_FAULT` | Generic monitor flag asserted |
| `MISTRIM` | Mistrim / XCD threshold exceeded |
| `VALIDITY_LOSS` | Sensor/data validity lost pre-trigger |
| `ENFORCE_STANDBY` | EnforceStandby asserted (software-commanded) |
| `CAP_LOST` | Upstream capability lost (cascade) |
| `UNKNOWN` | No matching pattern found |

TOGA or `apQuickDisconnect` rising pre-trigger while AFCS is engaged (`afcsEngage`/`afcsEngageCws` active) is classified as `PILOT_CMD` with label **Pilot Deactivation (TOGA/QD)**.

> Torque Limiting is excluded from fault classification — it is a protection mechanism, not a fault.

### Flight phases

When `mode_transitions` is present in the JSON, the tool automatically detects and renders
phase cards for three phase types:

#### TAKEOFF (amber)
`latActive = takeoff` and/or `vertActive = takeoff` and/or `atActive = takeOff`.
Data sourced from `takeoff_plots`. Chart window starts 10 s before the first takeoff
mode activation and extends 30 s past the last deactivation to capture the climb-out.

#### APPROACH (purple) / LANDING (teal)
- **APPROACH** — `latActive = navAppr` and/or `vertActive = glidePath`
- **LANDING** — `vertActive = flare`, `latActive = align`, or `atActive = retard`

Approach and Landing are merged into a single combined card (blue) when they are adjacent.
The chart window starts 10 s before the first approach mode activation.

Data sourced from `flight_plots`. Falls back to per-episode ±30 s windows when
`flight_plots` is absent (older JSONs).

#### Phase card contents

Each phase card shows:
- Sortie filename, phase type badge, active mode names, time range (HH:MM:SS), duration
- Entry/exit AGL, entry CAS, CAS target, ground speed, flap, heading
- Lateral and vertical deviation — max absolute, RMS, chart with mode transition markers
- Pitch and roll tracking — actual vs commanded (solid vs dashed), max/RMS error
- Touchdown lat/lon with Google Maps link (landing phase only)
- Any AFCS faults that occurred during the phase

#### Charts

All three charts in a phase card (deviation, pitch, roll) share the same x-axis and
behave as a linked group:

- **Scroll to zoom** — zooms the x-axis on all charts simultaneously
- **Double-click** — resets zoom on all charts
- **Synchronized crosshair** — hovering over any chart shows the cursor and value
  readout on all three charts at the same time position
- **Y-axis auto-scales** to the min/max of the data visible in the current zoom window
- **Rad Alt overlay** on the deviation chart — right y-axis (0 ft at bottom), drawn in blue
- **X-axis labels** — elapsed seconds from approach/takeoff start, with HH:MM:SS.s
  IRIG time shown at the edges and midpoint
- **Mode transition markers** — dashed vertical lines labeled with the mode name,
  colored by axis (LAT purple, VERT teal, AT amber)

### Data Summary view

Accessible via the **Data Summary** queue item. Shows:

**Score row** — fleet totals across all loaded sorties:
- **Total faults** — all AFCS capable disengagements
- **Sorties** / **Sorties affected** — loaded count and count with at least one fault
- **Approaches** — APPROACH phases detected (navAppr active)
- **Auto landings** — LANDING phases with flare or align active
- **Data gaps** — simultaneous multi-signal flat segments ≥5 s across APPROACH/LANDING phases (amber; only shown when >0)

**Fault Category Distribution** — stacked bar chart per category, colored by sortie.

**Faults by Sortie** table — all sorties in a single aligned table with columns:
Ep · Category · Evidence · Phase · Axis · Conf.

The **Phase** column shows what flight phase was active at fault time:
- `APPR` (purple) — fault occurred during an approach
- `LDG` (teal) — fault occurred during a landing
- `T/O` (amber) — fault occurred during takeoff
- `—` — fault occurred outside any detected phase

Confidence (`Conf.`) column:
- **✓** — fault confirmed (debounce output found, or validity loss by definition)
- **✗** — category identified but no debounce confirmation signal found in window
- **—** — pilot-commanded or unknown

### Signal Transitions by Lane

Each fault episode shows concurrent transitions grouped by FCC lane (FCC1A, FCC1B first; non-FCC lanes below):

- **Model name** shown in muted text alongside each signal suffix (e.g., `afcsCapable fgafcscapable`) — hover the cell for the full signal path
- **Enum transitions** (non-bool from/to values) shown in blue with actual value e.g. `0→2`
- **Chain filter** button (requires trace graph) — dims signals not in the BFS upstream chain of the fault trigger; shows hop-distance badge on in-chain signals

### Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Exclude signals | `apQuickDisconnect, togaPb, apPb` | Episodes containing these are treated as pilot-commanded and hidden |
| Max concurrent signals shown | 10 | Limits the concurrent signal list length per episode (hold ± buttons to ramp) |

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

## Publishing results

Analysis JSONs are gitignored on `master` to keep the repo lean. Use the `data` branch
to share results with the team.

### One-time setup

```bash
git checkout --orphan data
git rm -rf .
echo "*.zip" > .gitignore
git add .gitignore
git commit -m "Initialize data branch"
git push -u origin data
git checkout master
```

### Publish after each pipeline run

```bash
bash publish_data.sh
```

Each publish appends a new commit to `origin/data` — full history is preserved so you
can retrieve results from any previous run:

```bash
git log origin/data --oneline          # list all publishes
git checkout <hash> -- analysis_S114_1.json   # restore a specific file
```

Dry-run preview:
```bash
bash publish_data.sh --dry-run
```

---

## Temp disk space

Each ZIP is extracted to a temp CSV before parallel analysis, then deleted on completion.
With `parallel_sorties=3` and large ZIPs, peak temp usage can reach 20–40 GB.

- Clean up orphaned temp files after a killed run: `rm $TEMP/_rca_tmp_*.csv`
- Set `delete_zips_after: true` to free source ZIPs as each sortie finishes
- Reduce `parallel_sorties` if disk space is limited

---

## Back-filling missing signals

When a signal is absent from all bulk-export ZIPs for a sortie (i.e. it was never
included in the IADS data-group CSV for that flight), use the patch export workflow
to re-export only that signal and inject it into the existing `analysis_*.json`.

### Prerequisites

- [iads-export](https://github.com/merlinlabs/iads-export) repo with `ie_venv` activated
- AWS SSO authenticated (`aws sso login`)

### Single sortie (PowerShell)

```powershell
# 1. Export the missing signal from the raw IADS data
cd C:\path\to\iads-export
.\ie_venv\Scripts\Activate.ps1
python patch_export_local.py `
    --sortie S112N208B_2 `
    --data-group config/patch_sysNotEngage.csv `
    --out-dir C:\path\to\flight-test-analyzer\S112_2_N208B

# 2. Patch the signal into the analysis JSON
cd C:\path\to\flight-test-analyzer
python patch_signal_from_zip.py `
    S112_2_N208B\patch_sysNotEngage_S112N208B_2.zip `
    --signal sysNotEngage `
    --data-root S112_2_N208B
```

### All sorties at once

```powershell
cd C:\path\to\flight-test-analyzer
python patch_all_sorties.py `
    --iads-export-dir C:\path\to\iads-export `
    --data-group C:\path\to\iads-export\config\patch_sysNotEngage.csv `
    --workers 2
```

`patch_all_sorties.py` skips sorties that already have the signal, deletes the
intermediate ZIP after patching, and reports `✓`/`✗` per sortie as they complete.

Dry-run to preview which sorties would be processed:
```powershell
python patch_all_sorties.py ... --dry-run
```

### Adding a new signal

1. Create a data-group CSV in `iads-export/config/` with the full signal path(s):
   ```
   FCC1A.g_somemodule_mdlrefdw.rtb.mySignal
   FCC1B.g_somemodule_mdlrefdw.rtb.mySignal
   ```
2. Run `patch_all_sorties.py --signal mySignal --data-group config/patch_mySignal.csv`

### Sortie name mapping

| Local directory | S3 raw name |
|---|---|
| `S112_2_N208B` | `S112N208B_2` |
| `S101_N208B` | `S101N208B` |

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
