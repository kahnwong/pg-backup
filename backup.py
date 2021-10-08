from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
from log import Logger

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.postgresql:postgresql:42.2.14" pyspark-shell'

spark = SparkSession \
    .builder \
    .appName("Pyspark playground") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

log = Logger()

### read secrets
import json

with open('environments/secrets.json', 'r') as f:
    secrets = json.loads(f.read())
    
host = secrets['host']
db_name = secrets['db_name']
username = secrets['username']
password = secrets['password']

### read table_names
with open('resources/table_names.txt', 'r') as f:
    table_names = [i.strip() for i in f.readlines()]

### backup
for index, table_name in enumerate(table_names, 1):
    log.info(f'processing table {index}/{len(table_names)}')
    log.info(f'table_name: {table_name}')

    df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://{}:5432/{}".format(host, db_name)) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    log.info('writing to parquet...')

    df\
        .repartition(4)\
        .write\
        .parquet(f'backup/{table_name}', mode='overwrite')


    # break