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
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

#あとで消すやつ
for f in dbutils.fs.ls("dbfs:/mnt/"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

#あとで消すやつ
for f in dbutils.fs.ls("dbfs:/mnt/datalake/book/chapter03"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

#あとで消すやつ
for f in dbutils.fs.ls("dbfs:/FileStore/tables/data"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC use catalog hive_metastore;
# MAGIC drop database if exists taxidb cascade;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet file from DataFiles to chapter03

# COMMAND ----------

#%fs
#rm -r /mnt/datalake/book/chapter03/YellowTaxisParquet

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisParquet", recurse=True)
dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxis.delta", recurse=True)
dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisDelta", recurse=True)
dbutils.fs.rm("/user/hive/warehouse/taxidb.db", recurse=True)


# COMMAND ----------

#%fs
#ls -al 'dbfs:/data/'

for f in dbutils.fs.ls("dbfs:/FileStore/tables/data"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

#%fs
#cp ./YellowTaxisParquet /mnt/datalake/book/chapter03/YellowTaxisParquet

dbutils.fs.cp('/FileStore/tables/data/YellowTaxi','/mnt/datalake/book/chapter03/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet file, and write it out in Delta Format

# COMMAND ----------

#%fs
#rm -r /mnt/datalake/book/chapter03/YellowTaxisDelta

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisDelta", recurse=True)


# COMMAND ----------

#df = spark.read.format("parquet").load("/mnt/datalake/book/chapter03/YellowTaxisParquet")
#df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter03/YellowTaxisDelta/")

df = spark.read.format("parquet").load("/FileStore/tables/data/YellowTaxi/yellow_tripdata_2022_01-1.parquet")
display(df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Copy the YellowTaxisLargeAppend.csv file to the chapter03 sub-folder

# COMMAND ----------

#%fs
#cp /mnt/datalake/book/DataFiles/YellowTaxisLargeAppend.csv /mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv

#dbutils.fs.cp('dbfs:/FileStore/tables/YellowTaxisLargeAppend.csv','dbfs:/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv')


# COMMAND ----------

for f in dbutils.fs.ls("dbfs:/mnt/datalake/book/chapter03"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC To do:
# MAGIC 1. Make sure that we copy the taxi_rate_code.csv file into the right path

# COMMAND ----------

#後で消すやつ
for f in dbutils.fs.ls("dbfs:/databricks-datasets"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")
