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

#We could create a dataframe from SparkSession using createDataFrame()
dfFromRDDTwo = spark.createDataFrame(rdd).toDF(*columns)

# 2 - Create DataFrame from List Collection
# In this example we will see how to create DataFrame from a list using PySpark.
# Instead of using RDD we will use "data"
dfData = spark.createDataFrame(data).toDF(*columns)
