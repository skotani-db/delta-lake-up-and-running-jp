# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC
# MAGIC   Name:          chapter 03/06 - Generated Columns
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the GENERATED COLUMNS feature of Delta Lake
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Create a table with GENERATE ALWAYS AS columns
# MAGIC        2 - Insert a rows in the table, triggering the computation of the GENERATED columns
# MAGIC        3 - Perform a SELECT to illustrate that the GENERATED ALWAYS AS column 
# MAGIC        4 - An example of how you cannot use a non-deterministic function to calculate a GENERATED column
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - YellowTaxisテーブルのシンプルバージョンを作成する

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE taxidb.YellowTaxis
# MAGIC (
# MAGIC     RideId               INT        COMMENT 'This is our primary Key column',
# MAGIC     VendorId             INT,
# MAGIC     PickupTime           TIMESTAMP,
# MAGIC     PickupYear           INT        GENERATED ALWAYS AS(YEAR  (PickupTime)),
# MAGIC     PickupMonth          INT        GENERATED ALWAYS AS(MONTH (PickupTime)),
# MAGIC     PickupDay            INT        GENERATED ALWAYS AS(DAY   (PickupTime)),
# MAGIC     DropTime             TIMESTAMP,
# MAGIC     CabNumber            STRING     COMMENT 'Official Yellow Cab Number'             
# MAGIC ) USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxis.delta"
# MAGIC COMMENT 'Table to store Yellow Taxi data'

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - テーブルにレコードを挿入すると、生成された列の計算がトリガーされます

# COMMAND ----------

# MAGIC %sql
# MAGIC -- レコードを挿入し、GENRATED 列の計算をトリガーします。
# MAGIC INSERT INTO taxidb.YellowTaxis
# MAGIC     (RideId, VendorId, PickupTime, DropTime, CabNumber)
# MAGIC VALUES
# MAGIC     (5, 101, '2021-7-1T8:43:28UTC+3', '2021-7-1T8:43:28UTC+3', '51-986')

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Perform a select of the relevant columns to ensure that our GENERATED columns are correct

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 生成された列が正しく計算されたことを示します
# MAGIC SELECT PickupTime, PickupYear, PickupMonth, PickupDay FROM taxidb.YellowTaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - 無効な GENERATED ALWAYS AS 関数の例 - UUID は非決定的である

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ここでは、GUID 主キーを持つテーブルを作成しようとしています
# MAGIC CREATE OR REPLACE TABLE default.dummy
# MAGIC (
# MAGIC     ID   STRING GENERATED ALWAYS AS (UUID()),
# MAGIC     Name STRING
# MAGIC ) USING DELTA

# COMMAND ----------


