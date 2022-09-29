###In this file we will see different ways to create a DataFrame using PySpark

# 0 - Import  SparkSession
from pyspark.sql import SparkSession

# 1 - First let's create a DataFrame from RDD
# https://www.databricks.com/glossary/what-is-rdd#:~:text=RDD%20was%20the%20primary%20user,that%20offers%20transformations%20and%20actions.
# RDD was the primary user-facing API in Spark since its inception. 
# At the core, an RDD is an immutable distributed collection of elements of your data
# partitioned across nodes in your cluster that can be operated 
# in parallel with a low-level API that offers transformations and actions
columns = ["fruit","fruit_count"]
data = [("Banana", "300"), ("Strawberry", "650"), ("Grapes", "3000"), ("Pineapple", "3000")]

spark = SparkSession.builder.appName('CastroExample').getOrCreate()
rdd = spark.sparkContext.parallelize(data)
# Using function toDF()
dfFromRDD = rdd.toDF(columns)
dfFromRDD.printSchema()

# We could create a dataframe from SparkSession using createDataFrame()
dfFromRDDTwo = spark.createDataFrame(rdd).toDF(*columns)

# 2 - Create DataFrame from List Collection
# In this example we will see how to create DataFrame from a list using PySpark.
# Instead of using RDD we will use "data"
dfData = spark.createDataFrame(data).toDF(*columns)

# Another way of using createDataFrame() is with the Row Type
# two arguments are required Rows and Schema (columns), but before use List as rows 
# we need to convert our lista (data) into Row type
rows = map(lambda x: Row(*x), data)
dfRows = spark.createDataFrame(rows, columns)

# 3 - Working with schema
# If you want specify the column datatype or names, you should use StructType schema 
# First you need import types
from pyspark.sql.types import StructType, StructField, StringType, IntergerType

data_input = [
  ("bmw", "x1", "2005", 50000),
  ("bmw", "x3", "2008", 90000),
  ("Audi", "a1", "2017", 75000),
  ("Mercedez", "", "2020", 150000),
]

schema = StructType([
  StructField("Brand", StringType(), True),
  StructField("Model", StringType(), True),
  StructField("Year", StringType(), True),
  StructField("Price", IntegerType(), True),
])

df = spark.createDataFrame(data=data_input, schema=schema)
df.show(truncate=False)

# 4 - Using a CSV as a sourch
df_csv = spark.read.csv("your_path_here/file.csv")

# 5 - Using a JSON file as a sourch
df_json = spark.read.json("your_path_here/file.json")

# 6 - Using a TXT file as a sourch
df_txt = spark.read.txt("your_path_here/file.txt")

# 7 - Using a Parquet file as a sourch
df_json = spark.read.parquet("your_path_here/file.parquet")
