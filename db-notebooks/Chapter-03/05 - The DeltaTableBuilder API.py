# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 03/05 - The DeltaTableBuilder API
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the powerful DeltaTableBuilder API to create Delta tables
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Create a managed table with the DeltaTableBuilder API
# MAGIC        2 - Use the DESCRIBE EXTENDED command to study the managed table
# MAGIC        3 - Drop the table, so we can re-create it as an unmanaged table
# MAGIC        4 - Create an unmanaged table with the DeltaTableBuilder API
# MAGIC        5 - Use the DESCRIBE EXTENDED command to study to unmanaged table
# MAGIC
# MAGIC    

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. DeltaTableBuilder API を使用してマネージド・テーブルを作成する

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# DBTITLE 1,Create the greentaxis table as an unmanaged table
#
# このテーブル作成では、場所は指定しない。
# MANAGEDテーブルを作成している
#
DeltaTable.createIfNotExists(spark)                              \
    .tableName("taxidb.greenTaxis")                              \
    .addColumn("RideId", "INT", comment = "Primary Key")         \
    .addColumn("VendorId", "INT", comment = "Ride Vendor")       \
    .addColumn("EventType", "STRING")                            \
    .addColumn("PickupTime", "TIMESTAMP")                        \
    .addColumn("PickupLocationId", "INT")                        \
    .addColumn("CabLicense", "STRING")                           \
    .addColumn("DriversLicense", "STRING")                       \
    .addColumn("PassengerCount", "INT")                          \
    .addColumn("DropTime", "TIMESTAMP")                          \
    .addColumn("DropLocationId", "INT")                          \
    .addColumn("RateCodeId", "INT", comment = "Ref to RateCard") \
    .addColumn("PaymentType", "INT")                             \
    .addColumn("TripDistance", "DOUBLE")                         \
    .addColumn("TotalAmount", "DOUBLE")                          \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. DESCRIBE EXTENDED コマンドを使用して、上記で作成したマネージドテーブルを調べます。

# COMMAND ----------

# DBTITLE 0,Detailed look at the created table
# MAGIC %sql
# MAGIC --
# MAGIC -- テーブルを見てみましょう
# MAGIC --
# MAGIC DESCRIBE TABLE EXTENDED taxidb.greentaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. ###3. テーブルを削除し、外部テーブルとして再作成します。

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.greentaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. DeltaTableBuilder API を使用して外部テーブルを作成する

# COMMAND ----------

# DBTITLE 1,Create the greentaxis table as an unmanaged table
#
# このテーブル作成では、場所を指定している。
# 外部テーブルを作成している
#
DeltaTable.createIfNotExists(spark)                              \
    .tableName("taxidb.greenTaxis")                              \
    .addColumn("RideId", "INT", comment = "Primary Key")         \
    .addColumn("VendorId", "INT", comment = "Ride Vendor")       \
    .addColumn("EventType", "STRING")                            \
    .addColumn("PickupTime", "TIMESTAMP")                        \
    .addColumn("PickupLocationId", "INT")                        \
    .addColumn("CabLicense", "STRING")                           \
    .addColumn("DriversLicense", "STRING")                       \
    .addColumn("PassengerCount", "INT")                          \
    .addColumn("DropTime", "TIMESTAMP")                          \
    .addColumn("DropLocationId", "INT")                          \
    .addColumn("RateCodeId", "INT", comment = "Ref to RateCard") \
    .addColumn("PaymentType", "INT")                             \
    .addColumn("TripDistance", "DOUBLE")                         \
    .addColumn("TotalAmount", "DOUBLE")                          \
    .property("description", "table with Green Taxi Data")       \
    .location("/mnt/datalake/book/chapter03/greenTaxi")          \
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - 管理されていない taxidb.greenTaxis テーブルを調べるには DESCRIBE EXTENDED を使用します。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED taxidb.greenTaxis

# COMMAND ----------


