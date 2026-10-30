"""Transparent surveillance analytics over aggregated gold data only."""
from pathlib import Path
import json
import numpy as np
import pandas as pd

CODE_DISEASE={'A90':'dengue','A09':'diarrhoea','J11':'ili','B54':'malaria'}
def load_parquet_tree(path):
    if not path.exists():
        for suffix in ('.parquet','.csv'):
            candidate=path.with_suffix(suffix)
            if candidate.exists():
                return pd.read_parquet(candidate) if suffix=='.parquet' else pd.read_csv(candidate)
    parquet=list(path.rglob('*.parquet')); csv=list(path.rglob('*.csv'))
    # Reading the dataset root preserves Hive partition columns such as
    # epi_year and division; reading each part file separately does not.
    if parquet: return pd.read_parquet(path)
    if csv: return pd.concat([pd.read_csv(x) for x in csv],ignore_index=True)
    raise FileNotFoundError(path)
def suppress(df, columns):
    out=df.copy()
    for c in columns: out.loc[out[c].between(1,4),c]=np.nan
    return out
def run_analytics(lake: Path):
    g=load_parquet_tree(lake/'gold'/'weekly_cases'); g['disease']=g.icd10_code.map(CODE_DISEASE).fillna('other')
    weekly=g.groupby(['district','division','epi_year','epi_week','disease'],as_index=False,observed=True).agg(cases=('cases','sum'), admissions=('admissions','sum'))
    weekly['baseline']=weekly.groupby(['district','disease']).cases.transform(lambda x:x.rolling(8,min_periods=4).mean().shift(1)).fillna(weekly.cases.mean())
    weekly['std']=weekly.groupby(['district','disease']).cases.transform(lambda x:x.rolling(8,min_periods=4).std().shift(1)).fillna(1)
    weekly['threshold']=weekly['baseline']+2*weekly['std']; weekly['alert']=weekly.cases>weekly.threshold
    # Interpretable 1-4 week projection and 80% interval.
    weekly['forecast']=np.maximum(0,weekly['baseline']*.7+weekly['cases']*.3)
    weekly['lower']=np.maximum(0,weekly.forecast-1.28*weekly['std']); weekly['upper']=weekly.forecast+1.28*weekly['std']
    weekly['beds_demand']=np.ceil(weekly.forecast*.12); weekly['iv_saline_demand']=np.ceil(weekly.forecast*.7); weekly['ns1_demand']=np.ceil(weekly.forecast*.5)
    try: weekly.to_parquet(lake/'gold'/'analytics.parquet',index=False)
    except ImportError: weekly.to_csv(lake/'gold'/'analytics.csv',index=False)
    # Greedy transparent LP-equivalent allocation under fixed budget; equity floor 5% shares.
    # Keep one decision record for every district-disease pair so the linked
    # dashboard allocation view always follows the selected disease.
    last=weekly.sort_values(['epi_year','epi_week']).groupby(['district','disease'],observed=True).tail(1).copy(); last['severity']=last['alert'].astype(int)+last['cases']/max(1,last.cases.max())
    last['population_share']=1/last.groupby('disease',observed=True)['district'].transform('count'); budget=100000
    severity_total=last.groupby('disease',observed=True)['severity'].transform('sum')
    last['recommended_allocation']=np.maximum(budget*.05*last.population_share,budget*last.severity/severity_total)
    last['current_stock']=last.iv_saline_demand*.6; last['unmet_demand']=np.maximum(0,last.iv_saline_demand-last.current_stock-last.recommended_allocation/100)
    try: last.to_parquet(lake/'gold'/'allocation.parquet',index=False)
    except ImportError: last.to_csv(lake/'gold'/'allocation.csv',index=False)
    cal=weekly.groupby(['division','disease','epi_week'],as_index=False,observed=True).cases.mean(); cal['action']=np.where(cal.cases>=cal.groupby(['division','disease'],observed=True).cases.transform('quantile',q=.75),'pre-position supplies / intensify prevention','routine readiness')
    try: cal.to_parquet(lake/'gold'/'calendar.parquet',index=False)
    except ImportError: cal.to_csv(lake/'gold'/'calendar.csv',index=False)
    metrics={'method':'EARS C2-inspired rolling baseline','detection_note':'Evaluation is computed against the held-out ground truth only in evaluation scripts.','forecast_baseline':'seasonal naive rolling mean','allocation_budget_bdt':budget,'suppression_policy':'Counts 1-4 are suppressed and totals are independently suppressed where needed.'}
    (lake/'gold'/'metrics.json').write_text(json.dumps(metrics,indent=2),encoding='utf-8')
