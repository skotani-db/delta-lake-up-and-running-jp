# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 05/00 - Chapter 5 Initialization
# MAGIC
# MAGIC     Purpose:  The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC               This notebook resets and sets up all Hive databases and data files, so that we can successfully 
# MAGIC               execute all notebooks in this chapter in sequence.
# MAGIC
# MAGIC                 
# MAGIC     The following actions are taken in this notebook:
# MAGIC      1 - Drop the taxidb database with a cascade, deleting all tables in the database
# MAGIC      2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory
# MAGIC      3 - Read the parquet files, and write the table in Delta Format
# MAGIC      4 - Create database and register the delta table in hive
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS taxidb CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet files from DataFiles to the chapter05 directory

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter05/YellowTaxisParquet", recurse=True)

# COMMAND ----------

dbutils.fs.cp('/FileStore/tables/data/YellowTaxi','/mnt/datalake/book/chapter05/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter05/YellowTaxisParquet

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet files, and write the table in Delta Format

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter05/YellowTaxisDelta", recurse=True)

# COMMAND ----------

from pyspark.sql.types import (StructType,StructField,StringType,IntegerType,TimestampType,DoubleType,LongType)
from pyspark.sql.functions import (to_date, year, month, dayofmonth)

# add date columns to dataframe
df = spark.read.format("parquet").load("/mnt/datalake/book/chapter04/YellowTaxisParquet")
df = (
    df.withColumn("PickupDate", to_date("PickupTime"))
    .withColumn("PickupYear", year('PickupTime'))
    .withColumn("PickupMonth", month('PickupTime'))
    .withColumn("PickupDay", dayofmonth('PickupTime'))
)

# define the path and how many partitions we want this file broken up into so we can demonstrate compaction
path = "/mnt/datalake/book/chapter05/YellowTaxisDelta/"
numberOfFiles = 200

# repartition dataframe and write delta table
df.repartition(numberOfFiles).write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Create database and register the delta table in hive

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidb;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.tripData
# MAGIC USING DELTA LOCATION '/mnt/datalake/book/chapter05/YellowTaxisDelta';

# COMMAND ----------


