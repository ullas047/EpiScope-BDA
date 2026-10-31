from pathlib import Path
import json, shutil
import pandas as pd
import numpy as np
def read(path):
    if path.is_dir():
        pq=list(path.rglob('*.parquet')); csv=list(path.rglob('*.csv'))
        return pd.read_parquet(path) if pq else pd.concat([pd.read_csv(x) for x in csv],ignore_index=True)
    return pd.read_parquet(path) if path.suffix=='.parquet' else pd.read_csv(path)
def safe_records(df, cols):
    result=df.copy()
    for c in cols: result.loc[result[c].between(1,4),c]=None
    return result.replace({np.nan:None}).to_dict(orient='records')
def export_dashboard(lake: Path, out: Path):
    out.mkdir(parents=True,exist_ok=True)
    def asset(name):
        directory=lake/'gold'/name
        if directory.exists(): return directory
        return lake/'gold'/f'{name}.parquet' if (lake/'gold'/f'{name}.parquet').exists() else lake/'gold'/f'{name}.csv'
    a=read(asset('analytics')); allocation=read(asset('allocation')); cal=read(asset('calendar')); q=read(asset('quality_report'))
    # Retain complete history in gold, but serve only the latest 26 weeks to
    # meet VIS-4's browser payload ceiling. Seasonal aggregates remain intact.
    max_year, max_week = int(a['epi_year'].max()), int(a.loc[a['epi_year']==a['epi_year'].max(), 'epi_week'].max())
    latest_ordinal=max_year*53+max_week
    a=a.assign(_ordinal=a['epi_year']*53+a['epi_week'])
    a=a[a['_ordinal']>=latest_ordinal-25].drop(columns='_ordinal')
    # Aggregated JSON only; names are maps keyed by district to keep serving compact (<10MB at full scale).
    payload={'generated_at':pd.Timestamp.now(tz='UTC').isoformat(),'history_window':'Latest 26 epidemiological weeks; complete history remains in gold Parquet.','suppression':'Suppressed: 1-4','weekly':safe_records(a,['cases','admissions']),'allocation':safe_records(allocation,['current_stock','unmet_demand']),'calendar':safe_records(cal,['cases']),'quality':safe_records(q,['rows_quarantined'])}
    text=json.dumps(payload,separators=(',',':'))
    if len(text)>10_000_000: raise RuntimeError('Serving export exceeds VIS-4 10MB limit')
    (out/'dashboard.json').write_text(text,encoding='utf-8')
    shutil.copy2(out/'dashboard.json',Path('dashboard')/'data.json')
    print(f'Exported {len(text)/1e6:.2f}MB privacy-safe dashboard data.')
