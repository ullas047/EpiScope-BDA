"""Twelve auditable quality rules; failing rows receive reason codes."""
from pyspark.sql import functions as F

VALID_CODES=['A90','A91','A00','A09','J10','J11','B50','B51','B52','B53','B54']
def validate(encounters, facilities):
    checks = [
      ('missing_encounter_id', F.col('encounter_id').isNull()), ('missing_patient_id', F.col('patient_id').isNull()),
      ('missing_facility_id', F.col('facility_id').isNull()), ('missing_onset_date', F.col('onset_date').isNull()),
      ('age_out_of_range', ~F.col('age_years').between(0,120)), ('invalid_sex', ~F.col('sex').isin('F','M')),
      ('invalid_icd10', ~F.col('icd10_code').isin(VALID_CODES)), ('onset_after_report', F.col('onset_date')>F.col('report_date')),
      ('invalid_report_delay', F.datediff('report_date','onset_date')>21), ('orphan_facility', F.col('registry_facility_id').isNull()),
      ('missing_district', F.col('district').isNull()), ('invalid_outcome', ~F.col('outcome').isin('recovered','death'))]
    # Bring only the registry attributes needed downstream. Avoid duplicating
    # `district`, which would make gold aggregation ambiguous.
    registry=facilities.select(F.col('facility_id').alias('registry_facility_id'),'division')
    joined=encounters.join(registry, encounters.facility_id==registry.registry_facility_id,'left')
    reason=F.concat_ws('|', *[F.when(expr,F.lit(name)) for name,expr in checks])
    return joined.withColumn('reason_code',reason), [name for name,_ in checks]
