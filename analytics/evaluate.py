"""Evaluation-only ground truth reader. Never imported by production pipeline."""
import pandas as pd
def detection_metrics(alerts_path, truth_path):
    alerts=pd.read_parquet(alerts_path); truth=pd.read_csv(truth_path)
    matched=sum(((alerts.district==o.district)&(alerts.alert)&(alerts.disease==o.disease)).any() for o in truth.itertuples())
    return {'sensitivity':matched/len(truth),'false_alerts_per_district_year':float(alerts.alert.mean())}
