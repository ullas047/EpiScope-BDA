"""Portable demo fallback used only when PySpark is unavailable on this computer.

The graded/full-scale path remains `run_pipeline.py`'s PySpark implementation.
"""
from pathlib import Path
import hashlib, json, os
import pandas as pd

VALID={'A90','A91','A00','A09','J10','J11','B50','B51','B52','B53','B54'}
def _hash(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def run_pipeline_fallback(raw: Path, lake: Path):
    lake.mkdir(parents=True,exist_ok=True)
    for p in ['bronze','silver','gold','quarantine']:(lake/p).mkdir(exist_ok=True)
    batch=_hash(raw/'encounters.csv')[:16]; manifest_path=lake/'bronze'/'manifest.csv'
    manifest=pd.read_csv(manifest_path) if manifest_path.exists() else pd.DataFrame(columns=['batch_id','source','checksum'])
    if batch not in set(manifest.batch_id):
        e=pd.read_csv(raw/'encounters.csv'); e['batch_id']=batch; e['ingested_at']=pd.Timestamp.now(tz='UTC').isoformat()
        bronze_path=lake/'bronze'/'encounters.csv'; e.to_csv(bronze_path,index=False,mode='a',header=not bronze_path.exists())
        pd.concat([manifest,pd.DataFrame([{'batch_id':batch,'source':'encounters.csv','checksum':_hash(raw/'encounters.csv')}])]).to_csv(manifest_path,index=False)
    e=pd.read_csv(lake/'bronze'/'encounters.csv'); f=pd.read_csv(raw/'facilities.csv')[['facility_id','division']]
    e=e.merge(f,on='facility_id',how='left',suffixes=('','_registry'))
    reasons=[]
    def flag(name, mask): reasons.append((name,mask.fillna(True)))
    flag('missing_encounter_id',e.encounter_id.isna()); flag('missing_patient_id',e.patient_id.isna()); flag('missing_facility_id',e.facility_id.isna()); flag('missing_onset_date',e.onset_date.isna()); flag('age_out_of_range',~e.age_years.between(0,120)); flag('invalid_sex',~e.sex.isin(['F','M'])); flag('invalid_icd10',~e.icd10_code.isin(VALID)); flag('onset_after_report',pd.to_datetime(e.onset_date)>pd.to_datetime(e.report_date)); flag('invalid_report_delay',(pd.to_datetime(e.report_date)-pd.to_datetime(e.onset_date)).dt.days>21); flag('orphan_facility',e.division.isna()); flag('missing_district',e.district.isna()); flag('invalid_outcome',~e.outcome.isin(['recovered','death']))
    e['reason_code']=''
    for name,mask in reasons:e.loc[mask,'reason_code']+=name+'|'
    bad=e[e.reason_code.ne('')]; good=e[e.reason_code.eq('')].copy(); bad.to_csv(lake/'quarantine'/'encounters.csv',index=False)
    salt=os.getenv('EPISCOPE_HASH_SALT','DEMO_ONLY_CHANGE_ME'); good['patient_hash']=(salt+':'+good.patient_id).map(lambda x:hashlib.sha256(x.encode()).hexdigest()); good=good.drop(columns=['patient_id','reason_code']).drop_duplicates('encounter_id',keep='last'); good['onset_date']=pd.to_datetime(good.onset_date);good['epi_year']=good.onset_date.dt.year;good['epi_week']=good.onset_date.dt.isocalendar().week.astype(int)
    good.to_csv(lake/'silver'/'encounters.csv',index=False)
    gold=good.groupby(['district','division','epi_year','epi_week','icd10_code'],as_index=False).agg(cases=('encounter_id','count'),admissions=('admitted','sum'),bronze_batch_ids=('batch_id','first'));gold.to_csv(lake/'gold'/'weekly_cases.csv',index=False)
    pd.DataFrame([{'batch_id':batch,'rows_in':len(e),'rows_valid':len(good),'rows_quarantined':len(bad),'rules_evaluated':len(reasons)}]).to_csv(lake/'gold'/'quality_report.csv',index=False)
    print('WARNING: executed portable pandas fallback because PySpark is not installed. Install requirements for required Spark mode.')
