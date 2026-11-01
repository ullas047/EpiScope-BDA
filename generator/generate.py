"""Deterministic synthetic S4-S6 generator. Outputs deliberately contain quality defects."""
from pathlib import Path
import json
import numpy as np
import pandas as pd

DIVISIONS = ['Barishal','Chattogram','Dhaka','Khulna','Mymensingh','Rajshahi','Rangpur','Sylhet']
DISEASES = {'dengue':'A90','diarrhoea':'A09','ili':'J11','malaria':'B54'}

def districts():
    names = ['Bagerhat','Bandarban','Barguna','Barishal','Bhola','Bogura','Brahmanbaria','Chandpur','Chapainawabganj','Chattogram','Chuadanga','Comilla','Coxsbazar','Dhaka','Dinajpur','Faridpur','Feni','Gaibandha','Gazipur','Gopalganj','Habiganj','Jamalpur','Jashore','Jhalokathi','Jhenaidah','Joypurhat','Khagrachhari','Khulna','Kishoreganj','Kurigram','Kushtia','Lakshmipur','Lalmonirhat','Madaripur','Magura','Manikganj','Meherpur','Moulvibazar','Munshiganj','Mymensingh','Naogaon','Narail','Narayanganj','Narsingdi','Natore','Netrokona','Nilphamari','Noakhali','Pabna','Panchagarh','Patuakhali','Pirojpur','Rajbari','Rajshahi','Rangamati','Rangpur','Satkhira','Shariatpur','Sherpur','Sirajganj','Sunamganj','Sylhet','Tangail','Thakurgaon']
    return pd.DataFrame({'district': names, 'division': [DIVISIONS[i % 8] for i in range(64)], 'population': [180_000 + (i * 73_217 % 1_100_000) for i in range(64)]})

def generate(out: Path, scale: float, seed: int):
    out.mkdir(parents=True, exist_ok=True); rng = np.random.default_rng(seed)
    d = districts(); d.to_csv(out/'population.csv', index=False)
    facilities = []
    for i in range(640):
        row=d.iloc[i % 64]; facilities.append({'facility_id':f'F{i:04d}','district':row.district,'division':row.division,'facility_type':['hospital','upazila_complex','clinic'][i%3],'beds':30+i%170,'icu_beds':i%20+2})
    f = pd.DataFrame(facilities); f.to_csv(out/'facilities.csv',index=False)
    dates=pd.date_range('2021-01-01','2025-12-31',freq='D')
    weather=[]
    for x in d.itertuples():
        for day in dates[::7]:
            seasonal=max(0, np.sin((day.dayofyear-140)/365*2*np.pi))
            weather.append({'district':x.district,'date':str(day.date()),'rainfall_mm':round(10+80*seasonal+rng.gamma(2,3),1),'temperature_c':round(25+5*seasonal+rng.normal(0,1),1),'humidity':round(65+20*seasonal+rng.normal(0,3),1)})
    pd.DataFrame(weather).to_json(out/'weather.json',orient='records')
    # SF=1 target is >=25m. Demo caps only the observed historical window, preserving scale formula.
    n=max(5000, int(25_000_000*scale)); rows=[]
    outbreak=[]
    for j in range(30):
        outbreak.append({'outbreak_id':f'OB{j:02d}','district':d.iloc[j*7%64].district,'disease':list(DISEASES)[j%4], 'start_date':str((pd.Timestamp('2021-03-01')+pd.Timedelta(days=j*53)).date()),'duration_days':14+j%35,'multiplier':2+j%4})
    odf=pd.DataFrame(outbreak); odf.to_csv(out/'ground_truth_outbreaks.csv',index=False)
    base_dates=rng.choice(dates, size=n); choices=rng.integers(0,64,n); dis=rng.choice(list(DISEASES),size=n,p=[.36,.32,.22,.10])
    for i in range(n):
        day=pd.Timestamp(base_dates[i]); dd=d.iloc[choices[i]]; delay=min(21,int(rng.gamma(2,2)))
        age=int(rng.integers(0,90)); facility=f.iloc[(choices[i]*10+rng.integers(0,10))%len(f)]
        rows.append({'encounter_id':f'E{seed}-{i:09d}','patient_id':f'NID{rng.integers(1, max(1000,n//4)):09d}','facility_id':facility.facility_id,'district':dd.district,'onset_date':str(day.date()),'report_date':str((day+pd.Timedelta(days=delay)).date()),'age_years':age,'sex':rng.choice(['F','M']),'icd10_code':DISEASES[dis[i]],'lab_confirmed':bool(rng.random()<.35),'admitted':bool(rng.random()<.12),'outcome':rng.choice(['recovered','death'],p=[.994,.006])})
    e=pd.DataFrame(rows)
    # Inject exactly the documented defects.
    e.loc[e.index[::50],'encounter_id']=e.loc[e.index[::50],'encounter_id'].shift(1).bfill() # ~2% duplicate ids
    e.loc[e.index[::200],'facility_id']='ORPHAN'
    e.loc[e.index[::100],'age_years']=212
    e.loc[e.index[::333],'icd10_code']='BAD'
    e.to_csv(out/'encounters.csv',index=False)
    stock=[]
    for z in f.itertuples(): stock.append({'facility_id':z.facility_id,'snapshot_date':'2025-12-31','iv_saline':int(rng.integers(20,400)),'ns1_kits':int(rng.integers(10,250)),'ors':int(rng.integers(40,600)),'antimalarials':int(rng.integers(10,150)),'occupied_beds':int(rng.integers(0,z.beds))})
    (out/'stocks.json').write_text(json.dumps(stock),encoding='utf-8')
    (out/'generator_metadata.json').write_text(json.dumps({'seed':seed,'scale_factor':scale,'rows':n,'synthetic':True},indent=2),encoding='utf-8')
    print(f'Generated {n:,} synthetic encounters (seed={seed}, SF={scale}).')

if __name__ == '__main__': generate(Path('data/raw'),.01,2026)
