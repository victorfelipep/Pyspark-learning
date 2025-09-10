from os import truncate
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .master('local')
    .appName('practice')
    .getOrCreate()
)

df = spark.read.csv('organizations-100.csv', header=True, inferSchema=True)
df.printSchema()

df_employees = df.select('Name','Country','Number of employees','Founded')
df_employees.where(col('Number of Employees').isNotNull()).filter(col('Founded') < 2005).orderBy('Number of Employees', ascending=False).show(5,truncate=False)

df.createOrReplaceTempView('organizations')
spark.sql("""
   select 
   `Name`,`Country`,`Number of employees`,`Founded` 
   from organizations""").show()