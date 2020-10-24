import argparse
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,expr,column,udf,array,lit

parser = argparse.ArgumentParser(description='read stored parquet file to dataframe')

parser.add_argument(
'--p_file',
type=str, 
dest='p_file', 
required=True,
help='parquet format file for reading'
)

args  = parser.parse_args()

p_file = args.p_file

spark = SparkSession.builder.appName("Parquet Reader").getOrCreate()

ipu_values = spark.read.format('parquet')\
    .load(f'{os.environ["IPU_PT"]}/{p_file}')

print(str(ipu_values.count()))

pd_ipu_values = ipu_values.select('*').toPandas()

print(str(pd_ipu_values.count()))


