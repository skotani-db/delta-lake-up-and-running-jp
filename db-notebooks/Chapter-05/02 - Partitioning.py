# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 05/02 - Chapter 5 - Partitioning
# MAGIC
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 5 of the book - Performance Tuning.
# MAGIC                 This notebook illustrates partitioning.
# MAGIC
# MAGIC                 
# MAGIC      The following actions are taken in this notebook:
# MAGIC        1 - Remove existing Delta table directory to remove all old files
# MAGIC        2 - Create a partitioned Delta Table
# MAGIC        3 - Update specified partitions
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 1 - Remove existing Delta table directory to remove all old files

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter05/YellowTaxisPartitionedDelta/", recurse=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Create a partitioned Delta Table

# COMMAND ----------

# DBTITLE 1,Create a partitioned Delta Table
# import month from sql functions
from pyspark.sql.functions import (month, to_date)

# define the source path and destination path
source_path = "/mnt/datalake/book/chapter05/YellowTaxisParquet"
destination_path = "/mnt/datalake/book/chapter05/YellowTaxisPartitionedDelta/"

# read the delta table, add columns to partitions on, and write it using a partition.
# make sure to overwrite the existing schema if the table already exists since we are adding partitions
spark.table('taxidb.tripData')                                 \
.withColumn('PickupMonth', month('PickupDate'))   \
.withColumn('PickupDate', to_date('PickupDate'))  \
.write                                                      \
.partitionBy('PickupMonth')                                 \
.format("delta")                                            \
.option("overwriteSchema", "true")                          \
.mode("overwrite")                                          \
.save(destination_path)

# register table in Hive
spark.sql(f"""CREATE TABLE IF NOT EXISTS taxidb.tripDataPartitioned USING DELTA LOCATION '{destination_path}' """)

# COMMAND ----------

# DBTITLE 1,List partitions for the table
# MAGIC %sql
# MAGIC --list all partitions for the table
# MAGIC SHOW PARTITIONS taxidb.tripDataPartitioned

# COMMAND ----------

# DBTITLE 1,Show partitions in the underlying file system
# import OS module
import os

# list files and directories in directory 
print([d.name for d in dbutils.fs.ls(destination_path)])

# COMMAND ----------

json_files = []
log_files = dbutils.fs.ls("/mnt/datalake/book/chapter05/YellowTaxisPartitionedDelta/_delta_log/")
for file_info in log_files:
    if file_info.path.endswith('.json'):
        json_files.append(file_info.path)

dbutils.fs.cp(json_files[-1], f"file:/tmp/source.json")

# COMMAND ----------

# DBTITLE 1,Show AddFile metadata entry for partition
# MAGIC %sh
# MAGIC # find the last transaction entry and search for "add" to find an added file
# MAGIC # the output will show you the partitionValues
# MAGIC grep "\"add"\" "$(ls -1rt /tmp/source.json | tail -n1)" | sed -n 1p > /tmp/commit.json
# MAGIC python -m json.tool < /tmp/commit.json

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Update specified partitions

# COMMAND ----------

# DBTITLE 1,Use replaceWhere to update a specified partition
# import month from sql functions
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType

# use replaceWhere to update a specified partition
spark.read                                                              \
    .format("delta")                                                    \
    .load(destination_path)                                             \
    .where("PickupMonth == 2 and PaymentType == 4 ")              \
    .withColumn("PaymentType", lit(3).cast(IntegerType()))                \
    .write                                                              \
    .format("delta")                                                    \
    .option("replaceWhere", "PickupMonth = 2")                       \
    .mode("overwrite")                                                  \
    .save(destination_path)

# COMMAND ----------

# DBTITLE 1,Perform compaction on a specified partition
# read a partition from the delta table and repartition it 
spark.read.format("delta")          \
.load(destination_path)             \
.where("PickupMonth = 12 ")       \
.repartition(5)                     \
.write                              \
.option("dataChange", "false")      \
.format("delta")                    \
.mode("overwrite")                  \
.save(destination_path)

# COMMAND ----------

# DBTITLE 1,Optimize and ZORDER BY on a specified partition
# MAGIC %sql
# MAGIC OPTIMIZE taxidb.tripDataPartitioned WHERE PickupMonth = 12 ZORDER BY PickupTime

# COMMAND ----------


