"""Portable EpiScope orchestration entrypoint."""
import argparse
import os
import sys
from pathlib import Path

from generator.generate import generate
from analytics.run_analytics import run_analytics
from serving.export import export_dashboard

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale', type=float, default=0.01)
    parser.add_argument('--seed', type=int, default=2026)
    parser.add_argument('--incremental', type=Path)
    args = parser.parse_args()
    # Ensure Spark worker processes use the exact interpreter that launched the app.
    os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
    os.environ.setdefault('PYSPARK_DRIVER_PYTHON', sys.executable)
    raw = args.incremental or Path('data/raw')
    if not args.incremental:
        generate(raw, args.scale, args.seed)
    # Keep the portable demo output separate from the verified Spark lake.
    lake = Path(os.getenv('LAKE_ROOT', 'data/lake_spark'))
    try:
        from pipeline.run_pipeline import run_pipeline
        run_pipeline(raw, lake)
    except ModuleNotFoundError as exc:
        if exc.name != 'pyspark': raise
        from pipeline.fallback import run_pipeline_fallback
        run_pipeline_fallback(raw, lake)
    run_analytics(lake)
    export_dashboard(lake, Path('data/serving'))
    print('EpiScope complete. Serve dashboard with: python -m http.server 8080 --directory dashboard')

if __name__ == '__main__':
    main()
