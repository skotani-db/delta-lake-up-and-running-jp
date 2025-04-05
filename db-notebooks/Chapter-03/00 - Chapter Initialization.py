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

# MAGIC %fs
# MAGIC ls /FileStore/tables/data

# COMMAND ----------

#%fs
#cp ./YellowTaxisParquet /mnt/datalake/book/chapter03/YellowTaxisParquet

dbutils.fs.cp('/FileStore/tables/data/YellowTaxi','/mnt/datalake/book/chapter03/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet file, and write it out in Delta Format

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisDelta", recurse=True)


# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/book/chapter03/YellowTaxisParquet")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter03/YellowTaxisDelta/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Copy the YellowTaxisLargeAppend.csv file to the chapter03 sub-folder

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv", recurse=True)

# COMMAND ----------

try:
    dbutils.fs.cp('dbfs:/FileStore/tables/data/YellowTaxisLargeAppend.csv','dbfs:/mnt/datalake/book/chapter03/YellowTaxisLargeAppend.csv')
except:
    print("ファイルが存在しません。dbfs:/FileStore/tables/dataへYellowTaxisLargeAppend.csvファイルをアップロードしてください。")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Create the YellowTaxis_append.csv file to the chapter03 sub-folder

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/book/chapter03/YellowTaxis_append.csv", recurse=True)

# COMMAND ----------

import pandas as pd

# データの作成
data = {
   'RideId': [9999996, 9999997, 9999998, 9999999],
   'VendorId': [1, 1, 1, 1],
   'PickupTime': ['2019-01-01T00:00:00.000Z', '2019-01-01T00:00:00.000Z', '2019-01-01T00:00:00.000Z', '2019-01-01T00:00:00.000Z'],
   'DropTime': ['2022-01-01T00:13:13.000Z', '2022-01-01T00:09:21.000Z', '2022-01-01T00:09:15.000Z', '2022-01-01T00:10:01.000Z'],
   'PickupLocationId': [170, 161, 141, 161],
   'DropLocationId': [140, 68, 170, 68],
   'CabNumber': ['TAC399', 'T489328C', 'T509308C', 'VG354'],
   'DriverLicenseNumber': [5131685, 5076150, 5067782, 5012911],
   'PassengerCount': [1, 1, 0, 2],
   'TripDistance': [2.9, 1.1, 1.7, 2.86],
   'RateCodeId': [1, 1, 1, 1],
   'PaymentType': [1, 2, 1, 1],
   'TotalAmount': [15.3, 9.8, 12.35, 18.17],
   'FareAmount': [13.0, 8.5, 9.0, 14.5],
   'Extra': [0.5, 0.5, 0.5, 0.5],
   'MtaTax': [0.5, 0.5, 0.5, 0.5],
   'TipAmount': [1.0, 0.0, 2.05, 2.37],
   'TollsAmount': [0.0, 0.0, 0.0, 0.0],
   'ImprovementSurcharge': [0.3, 0.3, 0.3, 0.3]
}

# データフレーム作成
df = pd.DataFrame(data)

# CSVファイルに出力
df.to_csv('/dbfs/mnt/datalake/book/chapter03/YellowTaxis_append.csv', index=False)

print("CSVファイルが正常に出力されました: /mnt/datalake/book/chapter03/YellowTaxis_append.csv")

# COMMAND ----------


