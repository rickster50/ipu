import argparse
import re
import numpy as np
import statistics as st
import decimal
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,expr,column,udf,array,lit

parser = argparse.ArgumentParser(description='processor for ipu liquid products')

parser.add_argument(
'--ts_dir',
type=str, 
dest='ts_dir', 
required=True,
help='directory from which all files with _ts.csv in their name will be consumed'
)

args  = parser.parse_args()

ts_dir = args.ts_dir

hello_schema = StructType( [\
    StructField("TICK_TIME",TimestampType(),False),
    StructField("SOURCE_CODE",StringType(),False),
    StructField("TICK_CODE",StringType(),False),
    StructField("VALUE", IntegerType(),True)
    ])

spark = SparkSession.builder.appName("IPU Processor").getOrCreate()

daily_ticks = spark.read\
    .format("csv")\
        .option("delimiter","|")\
            .schema(hello_schema)\
                .load(f'{ts_dir}/*ts.csv')

daily_ticks_comparison = daily_ticks\
    .groupBy('TICK_TIME','TICK_CODE')\
        .pivot('SOURCE_CODE')\
            .sum()\
                .sort('TICK_TIME')

#daily_ticks_comparison.show(20)

value_source_matcher = re.compile("vs_") 
value_sources = [c for c in daily_ticks_comparison.columns if value_source_matcher.match(c)]

def get_mean_(*cols):
    print(cols)
    x = st.mean([num for elem in cols for num in elem])
    return round(float(x),2)

get_mean = udf(get_mean_,FloatType())

def get_median_(*cols):

    x = st.median([num for elem in cols for num in elem])
    return round(float(x),2)

get_median = udf(get_median_,FloatType())

def get_median_(*cols):

    x = st.median([num for elem in cols for num in elem])
    return round(float(x),2)

get_median = udf(get_median_,FloatType())

def get_max_(*cols):
    x = max([num for elem in cols for num in elem])
    return round(float(x),2)

get_max = udf(get_max_,FloatType())

def get_min_(*cols):
    x = min([num for elem in cols for num in elem])
    return round(float(x),2)

get_min = udf(get_min_,FloatType())

ipu_value = daily_ticks_comparison\
    .withColumn('IPU_mean',get_mean(array(value_sources)))\
        .withColumn('IPU_med',get_median(array(value_sources)))\
            .withColumn('IPU_max',get_max(array(value_sources)))\
                .withColumn('IPU_min',get_median(array(value_sources)))\



ipu_value.show(20)


#expr(f'(vs_A+vs_B+vs_C+vs_D+vs_E)/{str(len(value_sources))}'))



#ipu_value.withColumn('IPU_Value','summary / 5').show(10)       

#print(str(daily_ticks_comparison.columns))

#daily_ticks.sort(col('TICK_TIME')).show(10)

# daily_ticks.show(5)
# daily_ticks.printSchema()

# daily_ticks.sort(col('TICK_TIME')).show(10)

# daily_ticks.describe().show()

# daily_ticks.groupBy('TICK_TIME',TICK_CODE').pivot('SOURCE_CODE').sum().show(20)




