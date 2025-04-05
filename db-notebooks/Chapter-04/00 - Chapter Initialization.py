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
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Table Deletes, Updates and Merges
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

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# DBTITLE 0,Drop the taxidb database and all of its tables
# MAGIC %sql
# MAGIC drop database if exists taxidb cascade

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Copy the YellowTaxisParquet file from DataFiles to chapter04

# COMMAND ----------

dbutils.fs.rm('/mnt/datalake/book/chapter04/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/data/YellowTaxi

# COMMAND ----------

dbutils.fs.cp('/FileStore/tables/data/YellowTaxi','/mnt/datalake/book/chapter04/YellowTaxisParquet', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Read the parquet file, and write it out in Delta Format

# COMMAND ----------

dbutils.fs.rm('/mnt/datalake/book/chapter04/YellowTaxisDelta', recurse=True)

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/book/chapter04/YellowTaxisParquet")
df.write.format("delta").mode("overwrite").save("/mnt/datalake/book/chapter04/YellowTaxisDelta")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Re-create the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, create the database taxidb
# MAGIC CREATE DATABASE taxidb

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Create the YellowTaxis table on top of our Delta File in Chapter04

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-create YellowTaxis as an unmanaged table
# MAGIC CREATE OR REPLACE TABLE taxidb.YellowTaxis
# MAGIC (
# MAGIC     RideId                  INT,
# MAGIC     VendorId                INT,
# MAGIC     PickupTime              TIMESTAMP,
# MAGIC     DropTime                TIMESTAMP,
# MAGIC     PickupLocationId        INT,
# MAGIC     DropLocationId          INT,
# MAGIC     CabNumber               STRING,
# MAGIC     DriverLicenseNumber     STRING,
# MAGIC     PassengerCount          INT,
# MAGIC     TripDistance            DOUBLE,
# MAGIC     RatecodeId              INT,
# MAGIC     PaymentType             INT,
# MAGIC     TotalAmount             DOUBLE,
# MAGIC     FareAmount              DOUBLE,
# MAGIC     Extra                   DOUBLE,
# MAGIC     MtaTax                  DOUBLE,
# MAGIC     TipAmount               DOUBLE,
# MAGIC     TollsAmount             DOUBLE,         
# MAGIC     ImprovementSurcharge    DOUBLE
# MAGIC ) 
# MAGIC USING DELTA         
# MAGIC LOCATION "/mnt/datalake/book/chapter04/YellowTaxisDelta"

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE taxidb.YellowTaxis SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Get the count of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     COUNT(*)
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  
# MAGIC     *
# MAGIC FROM    
# MAGIC     taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Read the YellowTaxisMergeData.csv

# COMMAND ----------

dbutils.fs.cp('/FileStore/tables/data/YellowTaxisMergeData.csv','/mnt/datalake/book/chapter04/YellowTaxisMergeData.csv', recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter04/

# COMMAND ----------


