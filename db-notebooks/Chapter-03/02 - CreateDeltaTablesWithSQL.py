# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />  
# MAGIC
# MAGIC  
# MAGIC  Name:          chapter 03/02 - CreateDeltaTablesWithSql
# MAGIC
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to create Delta Tables with SQL
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Creating an unmanaged Delta table with SQL
# MAGIC        2 - Show the table name in the taxidb database
# MAGIC        3 - Show a directory listing for the location specified in the CREATE TABLE statement
# MAGIC        4 - Show the content of the table's transaction log directory
# MAGIC        5 - Show the metaData action in the 00000.json transaction log entry
# MAGIC        6 - Create an unmanaged Delta table with SQL
# MAGIC        7 - Show a directory listing of the the table in the /user/hive/warehouse directory
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - Deltaテーブルをfile_format`path_to_table`の指定で作成する

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deltaフォーマットを指定して、テーブルを作成する
# MAGIC -- パスをクオートで指定する
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/mnt/datalake/book/chapter03/rateCard`
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- taxidbデータベースでテーブルを作成する
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCard
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datalake/book/chapter03/rateCard'

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - Show the table in the taxidb database

# COMMAND ----------

# MAGIC %sql
# MAGIC -- taxidbデータベースのテーブルを表示します。今はrateCardテーブルだけを持っています。
# MAGIC -- 出力では名前が小文字になっていることに注意してください。Hiveは常にオブジェクト名
# MAGIC -- 小文字で保存されます。読みやすくするために、作者は引き続き
# MAGIC -- テーブルが最初に作成されたときに指定されたキャメルケースの名前を使用し続けます。
# MAGIC USE taxidb;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - テーブルのファイル・ディレクトリのリストを実行する

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter03/rateCard

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - テーブルのトランザクション・ログ・ディレクトリの内容を表示する

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter03/rateCard/_delta_log

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - トランザクションログエントリーに**メタデータ**エントリーを表示する

# COMMAND ----------

dbutils.fs.cp("dbfs:/mnt/datalake/book/chapter03/rateCard/_delta_log/00000000000000000000.json", "file:/tmp/00000000000000000000.json")

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /tmp/00000000000000000000.json | grep metadata > /tmp/metadata.json
# MAGIC python -m json.tool /tmp/metadata.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - 新しいマネージドテーブルを作成

# COMMAND ----------

# MAGIC %sql
# MAGIC -- このCREATE TABLE文では、場所を指定していないマネージドテーブルが作成されます
# MAGIC -- このテーブルはhiveによって管理され
# MAGIC -- /user/hive/warehouse/<データベース名>.db/<テーブル名> ディレクトリに格納されます。
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCardManaged
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - マネージドテーブルのディレクトリ一覧を表示

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/taxidb.db/ratecardmanaged

# COMMAND ----------


