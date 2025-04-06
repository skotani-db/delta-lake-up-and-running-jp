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

%sql
use catalog hive_metastore;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###1 - Drop the taxidb database and all of its tables

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
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

# Spark DataFrameを使ったデータの作成
data = [
    {
        'RideId': 9999996,
        'VendorId': 1,
        'PickupTime': '2019-01-01T00:00:00.000Z',
        'DropTime': '2022-01-01T00:13:13.000Z',
        'PickupLocationId': 170,
        'DropLocationId': 140,
        'CabNumber': 'TAC399',
        'DriverLicenseNumber': 5131685,
        'PassengerCount': 1,
        'TripDistance': 2.9,
        'RateCodeId': 1,
        'PaymentType': 1,
        'TotalAmount': 15.3,
        'FareAmount': 13.0,
        'Extra': 0.5,
        'MtaTax': 0.5,
        'TipAmount': 1.0,
        'TollsAmount': 0.0,
        'ImprovementSurcharge': 0.3
    },
    {
        'RideId': 9999997,
        'VendorId': 1,
        'PickupTime': '2019-01-01T00:00:00.000Z',
        'DropTime': '2022-01-01T00:09:21.000Z',
        'PickupLocationId': 161,
        'DropLocationId': 68,
        'CabNumber': 'T489328C',
        'DriverLicenseNumber': 5076150,
        'PassengerCount': 1,
        'TripDistance': 1.1,
        'RateCodeId': 1,
        'PaymentType': 2,
        'TotalAmount': 9.8,
        'FareAmount': 8.5,
        'Extra': 0.5,
        'MtaTax': 0.5,
        'TipAmount': 0.0,
        'TollsAmount': 0.0,
        'ImprovementSurcharge': 0.3
    },
    {
        'RideId': 9999998,
        'VendorId': 1,
        'PickupTime': '2019-01-01T00:00:00.000Z',
        'DropTime': '2022-01-01T00:09:15.000Z',
        'PickupLocationId': 141,
        'DropLocationId': 170,
        'CabNumber': 'T509308C',
        'DriverLicenseNumber': 5067782,
        'PassengerCount': 0,
        'TripDistance': 1.7,
        'RateCodeId': 1,
        'PaymentType': 1,
        'TotalAmount': 12.35,
        'FareAmount': 9.0,
        'Extra': 0.5,
        'MtaTax': 0.5,
        'TipAmount': 2.05,
        'TollsAmount': 0.0,
        'ImprovementSurcharge': 0.3
    },
    {
        'RideId': 9999999,
        'VendorId': 1,
        'PickupTime': '2019-01-01T00:00:00.000Z',
        'DropTime': '2022-01-01T00:10:01.000Z',
        'PickupLocationId': 161,
        'DropLocationId': 68,
        'CabNumber': 'VG354',
        'DriverLicenseNumber': 5012911,
        'PassengerCount': 2,
        'TripDistance': 2.86,
        'RateCodeId': 1,
        'PaymentType': 1,
        'TotalAmount': 18.17,
        'FareAmount': 14.5,
        'Extra': 0.5,
        'MtaTax': 0.5,
        'TipAmount': 2.37,
        'TollsAmount': 0.0,
        'ImprovementSurcharge': 0.3
    }
]


# データフレーム作成
df = spark.createDataFrame(data)
df.write.csv("/mnt/datalake/book/chapter03/YellowTaxis_append.csv", header=True, mode="overwrite")

print("CSVファイルが正常に出力されました: /mnt/datalake/book/chapter03/YellowTaxis_append.csv")
