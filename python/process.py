from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,expr,column

hello_schema = StructType( [\
    StructField("TICK_TIME",TimestampType(),False),
    StructField("SOURCE_CODE",StringType(),False),
    StructField("TICK_CODE",StringType(),False),
    StructField("VALUE", IntegerType(),True)
    ])

spark = SparkSession.builder.appName("IPU Processor").getOrCreate()

daily_ticks = spark.read.format("csv").option("delimiter","|").schema(hello_schema).load("/media/data/t1/hello*ts.csv")
#print(str(daily_ticks.count()))
print(str(daily_ticks.count()))

daily_ticks.groupBy('TICK_TIME','TICK_CODE').pivot('SOURCE_CODE').sum().show(20)

# daily_ticks.show(5)
# daily_ticks.printSchema()

# daily_ticks.sort(col('TICK_TIME')).show(10)

# daily_ticks.describe().show()

# daily_ticks.groupBy('TICK_TIME',TICK_CODE').pivot('SOURCE_CODE').sum().show(20)




