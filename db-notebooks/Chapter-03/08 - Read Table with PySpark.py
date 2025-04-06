# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC
# MAGIC  Name:          chapter 03/08 - Read Table with PySpark
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to read a table using PySpark
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Using pySpark to get a record count of a Delta Table
# MAGIC        2 - Run a complex query in PySpark
# MAGIC        3 - Illustrate that .groupBy() creates a pyspark.sql.GroupedDate instance
# MAGIC            and not a DataFrame
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - pySpark を使用してレコード数を取得します

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# .table 形式を使用してテーブル全体を読み取ることができることに注意してください
df = spark.read.format("delta").table("taxidb.YellowTaxis")

# 千単位のフォーマッタにより、結果が少し読みやすくなります
print(f"Number of records: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Write a complex query in pySpark

# COMMAND ----------

# 使用する関数を必ずインポートしてください
from pyspark.sql.functions import col, avg, desc

# YellowTaxis をデータフレームに読み込みます
df = spark.read.format("delta").table("taxidb.YellowTaxis")

# pySpark で、group by、average、having、order by の同等のものを実行します
results = df.groupBy("VendorID")                          \
            .agg(avg("FareAmount").alias("AverageFare"))   \
            .filter(col("AverageFare") > 50)               \
            .sort(col("AverageFare").desc())               \
            .take(5)                                      

# これはリストであり DataFrame ではないため、結果を出力します
# リストの理解を使用して、結果を 1 行で出力できます
[print(result) for result in results]

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - .groupBy() が DataFrame ではなく pyspark.sql.GroupedDate インスタンスを作成することを示します

# COMMAND ----------

# groupBy を実行し、タイプを出力します
print(type(df.groupBy("CabNumber")))

# COMMAND ----------


