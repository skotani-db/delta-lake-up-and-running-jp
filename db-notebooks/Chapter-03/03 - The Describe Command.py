# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 03/03 - The Describe Commandl
# MAGIC  
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the SQL DESCRIBE command with both Hive databases and tables
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Running the DESCRIBE command on a Hive Database
# MAGIC        2 - Running the DESCRIBE command on a unmanaged Delta Table
# MAGIC        3 - Running the DESCRIBE EXTENDED command on a unmanaged Delta Table
# MAGIC        4 - Running the DESCRIBE EXTENDED command on a managed Delta Table
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - HiveデータベースでDESCRIBEコマンドを実行する

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データベースに対してDESCRIBEを実行すると、名前空間（別名、データベース名）、およびコメント
# MAGIC -- データベースの作成時に入力されたコメント、データベース内の管理されていないテーブルの場所
# MAGIC -- データベースの所有者が返される。
# MAGIC DESCRIBE DATABASE taxidb;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - DeltaテーブルのDESCRIBEコマンドの実行

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルに対してDESCRIBEコマンドを実行すると、カラム名とデータ型、そしてパーティション情報
# MAGIC -- とデータ型、パーティショニング情報が返される。
# MAGIC DESCRIBE TABLE taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - 外部テーブルでの DESCRIBE EXTENDED コマンドの実行

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE EXTENDEDコマンドを実行すると、追加のメタデータが返されます。
# MAGIC -- Hiveデータベース名、基礎となるDeltaテーブル プロパティなどです。
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - マネージドテーブルで DESCRIBE EXTENDED コマンドを実行する

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 管理されているテーブルに対してDESCRIBE EXTENDEDコマンドを実行すると、以下のことがわかります。
# MAGIC -- テーブルのファイルが /user/hive/warehouse ディレクトリの下にあることがわかります。
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCardManaged
