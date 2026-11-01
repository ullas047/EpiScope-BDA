from pathlib import Path
import hashlib, os, sys
from pyspark.sql import SparkSession, functions as F, Window
from pipeline.quality.rules import validate

def sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as x:
      for b in iter(lambda:x.read(1<<20),b''): h.update(b)
    return h.hexdigest()

def run_pipeline(raw: Path, lake: Path):
    salt=os.getenv('EPISCOPE_HASH_SALT','DEMO_ONLY_CHANGE_ME')
    # Conservative local defaults suit a 16GB Windows laptop and avoid spawning one
    # Python worker per logical processor on small development runs.
    workers=os.getenv('EPISCOPE_SPARK_WORKERS','4')
    spark=(SparkSession.builder.master(f'local[{workers}]').appName('episcope')
        .config('spark.sql.adaptive.enabled','true')
        .config('spark.sql.shuffle.partitions','8')
        .config('spark.driver.memory',os.getenv('EPISCOPE_SPARK_MEMORY','5g'))
        .config('spark.pyspark.python',sys.executable)
        .config('spark.pyspark.driver.python',sys.executable)
        .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR'); lake.mkdir(parents=True,exist_ok=True)
    batch=sha256(raw/'encounters.csv')[:16]
    enc=spark.read.option('header',True).option('inferSchema',True).csv(str(raw/'encounters.csv')).withColumn('batch_id',F.lit(batch)).withColumn('ingested_at',F.current_timestamp())
    fac=spark.read.option('header',True).option('inferSchema',True).csv(str(raw/'facilities.csv'))
    # Immutable bronze with source metadata. A known checksum is not appended again.
    manifest_path=lake/'bronze'/'manifest'
    # A failed previous job can leave an empty output directory. Only read a
    # dataset when it contains actual Parquet part files.
    has_manifest_parts = manifest_path.exists() and any(manifest_path.rglob('part-*.parquet'))
    known = has_manifest_parts and spark.read.parquet(str(manifest_path)).filter(F.col('batch_id')==batch).limit(1).count()>0
    if not known:
        enc.write.mode('append').parquet(str(lake/'bronze'/'encounters'))
        manifest=spark.createDataFrame([(batch,'encounters.csv',sha256(raw/'encounters.csv'))],['batch_id','source','checksum'])
        manifest.write.mode('append').parquet(str(manifest_path))
    # Rebuild conformed layers from all immutable batches so a late drop updates prior epi-weeks.
    bronze=spark.read.parquet(str(lake/'bronze'/'encounters'))
    tested,names=validate(bronze,fac)
    bad=tested.filter(F.length('reason_code')>0); good=tested.filter(F.length('reason_code')==0)
    bad.write.mode('overwrite').parquet(str(lake/'quarantine'/'encounters'))
    # Hash PII then deduplicate idempotently. Patient id never enters silver.
    hashed=good.withColumn('patient_hash',F.sha2(F.concat_ws(':',F.lit(salt),F.col('patient_id')),256)).drop('patient_id','reason_code','beds','icu_beds','facility_type')
    silver=hashed.withColumn('rn',F.row_number().over(Window.partitionBy('encounter_id').orderBy(F.col('ingested_at').desc()))).filter('rn=1').drop('rn')
    silver= silver.withColumn('epi_year',F.year('onset_date')).withColumn('epi_week',F.weekofyear('onset_date'))
    silver.write.mode('overwrite').partitionBy('epi_year','division').parquet(str(lake/'silver'/'encounters'))
    quality=spark.createDataFrame([(batch,int(bronze.count()),int(good.count()),int(bad.count()),len(names))],['batch_id','rows_in','rows_valid','rows_quarantined','rules_evaluated'])
    quality.write.mode('overwrite').parquet(str(lake/'gold'/'quality_report'))
    gold=silver.groupBy('district','division','epi_year','epi_week','icd10_code').agg(F.count('*').alias('cases'),F.sum(F.col('admitted').cast('int')).alias('admissions'),F.first('batch_id').alias('bronze_batch_ids'))
    gold.write.mode('overwrite').partitionBy('epi_year','division').parquet(str(lake/'gold'/'weekly_cases'))
    spark.stop()
