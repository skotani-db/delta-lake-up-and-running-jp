# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 03/00 - Chapter 3 Initialization
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook resets all Hive databases and data files, so that we can successfully 
# MAGIC                 execute all notebooks in this chapter in sequence
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC        2 - ...
# MAGIC    

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - taxidbデータベースとそのテーブルを削除する

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC drop database if exists taxidb cascade;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - FileStoreからYellowTaxisデータのParquetを3章のフォルダへコピーする

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisParquet", recurse=True)
dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxis.delta", recurse=True)
dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisDelta", recurse=True)
dbutils.fs.rm("/user/hive/warehouse/taxidb.db", recurse=True)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/data

# COMMAND ----------

dbutils.fs.cp('/FileStore/tables/data/YellowTaxi','/mnt/datalake/book/chapter03/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Parquetファイルを読み込み、Deltaフォーマットへ変換する

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisDelta", recurse=True)

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/book/chapter03/YellowTaxisParquet")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter03/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - ellowTaxisLargeAppend.csvファイルを3章のフォルダへコピーする

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv", recurse=True)

# COMMAND ----------

try:
    dbutils.fs.cp('dbfs:/FileStore/tables/data/YellowTaxisLargeAppend.csv','dbfs:/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv')
except:
    print("ファイルが存在しません。dbfs:/FileStore/tables/dataへYellowTaxisLargeAppend.csvファイルをアップロードしてください。")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - YellowTaxis_append.csvを3章のフォルダへコピーする

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxis_append.csv", recurse=True)

# COMMAND ----------

try:
    dbutils.fs.cp('dbfs:/FileStore/tables/data/YellowTaxis_append.csv','dbfs:/mnt/datalake/book/chapter03/YellowTaxis_append.csv')
except:
    print("ファイルが存在しません。dbfs:/FileStore/tables/dataへYellowTaxis_append.csvファイルをアップロードしてください。")

# COMMAND ----------


