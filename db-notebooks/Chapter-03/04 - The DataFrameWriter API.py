# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />
# MAGIC
# MAGIC  
# MAGIC   Name:          chapter 03/04 - The DataFrameWriter API
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to use the DataFrameWriter API to create Delta tables
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Drop the taxidb.rateCard table
# MAGIC        2 - Read a CSV file into a DataFrame from the input path
# MAGIC        3 - Write the DataFrame to a managed Delta Table
# MAGIC        4 - Perform a DESCRIBE EXTENDED on the table to make sure that it is a managed table
# MAGIC        5 - Perform a SELECT on the table to ensure that the data was successfully loaded from the .csv input file
# MAGIC        6 - Drop the rateCard table so that you can re-created it as an unmanaged table
# MAGIC        7 - Write our DataFrame to your output path location
# MAGIC        8 - Create an unmanaged Delta table on top of the Delta File written in the previous step
# MAGIC        8 - Perform a SQL SELECT to show the records in the unmanaged table
# MAGIC
# MAGIC    

# COMMAND ----------

INPUT_PATH = '/FileStore/tables/data/nyctaxi/taxizone/taxi_rate_code.csv'
DELTALAKE_PATH = 'dbfs:/mnt/datalake/book/chapter03/createDeltaTableWithDataFrameWriter'


# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - taxidb.rateCardテーブルを削除する

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CSVファイルからtaxidb.rateCardテーブルを再作成します。
# MAGIC -- ファイルから再作成するので、まずそれをここにドロップする必要がある。
# MAGIC DROP TABLE IF EXISTS taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - taxi_rate_code.csvファイルを読み込む

# COMMAND ----------

# 入力パスからデータフレームを読み込む
df_rate_codes = spark                                              \
                .read                                              \
                .format("csv")                                     \
                .option("inferSchema", True)                       \
                .option("header", True)                            \
                .load(INPUT_PATH)

display(df_rate_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - ###3 - マネージド・デルタ・テーブルとして DataFrame を書き込む

# COMMAND ----------

# DataFrameをマネージドデルタテーブルとして保存する
# ロケーションパスが指定されていないため、テーブルがマネージドであることがわかる
df_rate_codes.write.format("delta").saveAsTable('taxidb.rateCard')

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - rateCard マネージドテーブルの DESCRIBE EXTENDED を実行する。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルのDESCRIBE EXTENDEDを実行すると、これがマネージドテーブルであることがわかる。
# MAGIC -- このテーブルに対してDESCRIBE EXTENDEDを実行すると、このテーブルがマネージドテーブルであることがわかります。
# MAGIC DESCRIBE TABLE EXTENDED taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - テーブルにSELECTを実行し、.CSVファイルからデータが正常にロードされたことを確認する

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidb.rateCard

# COMMAND ----------

# MAGIC %md
# MAGIC ###rateCard テーブルを削除し、アンマネージドテーブルとして再作成します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 既存のテーブルを削除する
# MAGIC drop table if exists taxidb.rateCard;

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Write our DataFrame to the output Data Lake Path

# COMMAND ----------

# 次に、デルタテーブルを作成する。
# パスとデルタテーブル名の両方を指定する
df_rate_codes                           \
        .write                          \
        .format("delta")                \
        .mode("overwrite")              \
        .option('path', DELTALAKE_PATH) \
        .saveAsTable('taxidb.rateCard')

# COMMAND ----------

# MAGIC %md
# MAGIC ###8 - テーブルのレコードを表示する

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 管理されていないテーブルから select を実行する。
# MAGIC select * from taxidb.rateCard
