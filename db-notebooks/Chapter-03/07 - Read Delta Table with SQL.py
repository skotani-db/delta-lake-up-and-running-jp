# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src= "https://cdn.oreillystatic.com/images/sitewide-headers/oreilly_logo_mark_red.svg"/>&nbsp;&nbsp;<font size="16"><b>Delta Lake: Up and Running<b></font></span>
# MAGIC <img style="float: left; margin: 0px 15px 15px 0px;" src="https://learning.oreilly.com/covers/urn:orm:book:9781098139711/400w/" />   
# MAGIC
# MAGIC  Name:          chapter 03/07 - Read Delta Table with SQL
# MAGIC  
# MAGIC      Author:    Bennie Haelen
# MAGIC      Date:      12-10-2022
# MAGIC      Purpose:   The notebooks in this folder contains the code for chapter 3 of the book - Basic Operations on Delta Tables.
# MAGIC                 This notebook illustrates how to read a table using SQL
# MAGIC
# MAGIC                 
# MAGIC      The following Delta Lake functionality is demonstrated in this notebook:
# MAGIC        1 - Create a Delta Table on top of a Delta File
# MAGIC        2 - Perform a record count with SQL
# MAGIC        3 - Perform a DESCRIBE FORMATTED listing of the Delta table
# MAGIC        4 - Illustrate the use of ANSI SQL in a more advanced query
# MAGIC        5 - Demonstrate the usage of Spark SQL in Python
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %md
# MAGIC ###1 - デルタ ファイル上にデルタ テーブルを作成する

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/datalake/book/chapter03/YellowTaxis.delta/

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 既存のデルタ ファイル上にデルタ テーブルを作成する構文
# MAGIC CREATE TABLE taxidb.YellowTaxis
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/datalake/book/chapter03/YellowTaxisDelta/"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###2 - SQL によるクイック レコード カウント

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*)
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - テーブルの DESCRIBE FORMATTED を実行する

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE FORMATTED taxidb.YellowTaxis;

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - より高度なクエリでの ANSI SQL の使用法を示す

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spark SQL が ANSI-SQL 構造をサポートしていることを実証する
# MAGIC SELECT 
# MAGIC     CabNumber,
# MAGIC     AVG(FareAmount) AS AverageFare
# MAGIC FROM
# MAGIC     taxidb.yellowtaxis
# MAGIC GROUP BY
# MAGIC     CabNumber
# MAGIC HAVING
# MAGIC        AVG(FareAmount)>50
# MAGIC ORDER BY
# MAGIC     2 DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Python での spark.sql の使用法を示す

# COMMAND ----------

number_of_results = 5

sql_statement = f"""
SELECT 
    CabNumber,
    AVG(FareAmount) AS AverageFare
FROM
    taxidb.yellowtaxis
GROUP BY
    CabNumber
HAVING
     AVG(FareAmount) > 50
ORDER BY
    2 DESC
LIMIT {number_of_results}"""

df = spark.sql(sql_statement)
display(df)

# COMMAND ----------


