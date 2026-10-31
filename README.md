# EpiScope

EpiScope is a reproducible prototype for district-level outbreak surveillance, short-horizon demand forecasting, and equitable response planning in Bangladesh. **All health records in this repository are synthetic.**


## One-command run

### Quick demo for this PC (recommended)

Double-click `RUN_DEMO.bat`. It installs only NumPy and pandas, runs the verified 25,000-record synthetic demo through the portable fallback, then starts the dashboard. Open `http://localhost:8080` and keep the command window open.

Or run these commands in PowerShell from the project folder:

```powershell
py -m pip install -r requirements-demo.txt
$env:EPISCOPE_HASH_SALT = "local-demo-only-change-before-real-use"
py run.py --scale 0.001 --seed 2026
py -m http.server 8080 --directory dashboard
```

### Full Spark pipeline

Use Python 3.11 for the full Spark path on this Windows laptop. PySpark 3.5.3 is verified here with Python 3.11; do not use the system Python 3.14.

Install the pinned dependencies, then run:

```powershell
make all SF=0.01 SEED=2026
```

On Windows without `make`, run `py -3.11 run.py --scale 0.01 --seed 2026`. This generates synthetic sources, creates bronze/silver/gold Parquet lake layers, evaluates analytics, and exports privacy-safe dashboard JSON. The default demo scale is intentionally small; use `SF=1` for the 25M-record contractual scale on a 16GB machine.

```powershell
py -3.11 -m pip install -r requirements.txt
python -m http.server 8080 --directory dashboard
```

Open `http://localhost:8080`. The dashboard never reads line-level data. To keep the browser payload below 10 MB, it serves the latest 26 epidemiological weeks; the complete history remains available in the gold Parquet lake.

## Hidden incremental drop

Place unchanged-schema CSV/JSON drops in `data/incremental/<batch-id>/` and run:

```powershell
python run.py --incremental data/incremental/<batch-id>
```

The bronze manifest uses SHA-256 checksums and silver deduplicates `encounter_id`; rerunning the same drop is idempotent. Late records are recomputed into their affected epidemiological weeks.

## Architecture

`generator -> bronze (immutable raw + manifest) -> silver (validated, de-identified, quarantined) -> gold (epi-week marts/features) -> analytics -> serving (suppressed aggregate JSON) -> D3 v7 dashboard`.

Heavy transforms are written with PySpark DataFrame APIs. Parquet is partitioned by `epi_year/division`; the default Spark lake is `data/lake_spark` and `LAKE_ROOT` can point to MinIO/S3A in deployment. No salt, secret, or API key is stored in Git; configure `EPISCOPE_HASH_SALT`.

## Requirement traceability

| Requirements | Evidence |
|---|---|
| ING-1–4 | `generator/generate.py`, `pipeline/run_pipeline.py` |
| DQ-1–3, PRV-1–2 | `pipeline/quality/rules.py`, `pipeline/silver/transform.py`, `serving/export.py` |
| AN-1–2, AN-4–7, AN-9 | `analytics/` and `docs/evaluation.md` |
| VIS-1–4 | `dashboard/index.html`, `dashboard/app.js` |
| ENG-1 | `Makefile`, `requirements.txt`, `run.py` |
| ENG-2–3 | `docs/performance.md` |

## Safety and reproducibility

Set `EPISCOPE_HASH_SALT` locally (see `.env.example`). The generator is deterministic for a given seed and scale. `ground_truth_outbreaks.csv` is deliberately excluded from pipeline reads; it is read only by `analytics/evaluate.py`.

## Local verification recorded on 2026-10-30

On 2026-10-31, the full PySpark path was verified with Python 3.11 and Java 8 at SF=.001: it generated 25,000 synthetic encounters, wrote bronze/silver/gold Parquet data, completed analytics, and exported a 7.26 MB privacy-safe dashboard file. `pytest` passed (3 tests). The SF=.25, .5 and 1 performance experiments remain separate measured runs required before submission; do not invent their results.

## Repository layout

See `docs/proposal.md` for the architecture, stakeholders, conflicts, risk register, and team-role placeholders. Replace all bracketed submission placeholders before tagging `v1.0`.
