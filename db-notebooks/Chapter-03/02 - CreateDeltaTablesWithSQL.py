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
# MAGIC ###1 - Create a Delta table using the file_format`path_to_table` specification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta table by specifying the delta format, followed
# MAGIC -- by the path in quotes
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/mnt/datalake/book/chapter03/rateCard`
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the table using the taxidb catalog
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
# MAGIC -- Show the tables in the taxidb database. We only have our rateCard table for now.
# MAGIC -- Notice the lowercase name in the output. Hive will always store its object names
# MAGIC -- in lower case. For readability purposes, the authors will continue to use the
# MAGIC -- CamelCase name specified when the table was first created.
# MAGIC USE taxidb;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - Run a directory listing on our table's files directory

# COMMAND ----------

#%sh
# Display the contents of the table's path. 
# Important note: Since we are running in a Databricks
# environment, we need to prefix our path with '/dbfs'
# Note that our directory is empty, since we have not 
# yet populated our rateCard table. Since we are using
# the Delta Lake format, we do see the _delta_log directory
# ls -al /dbfs/mnt/datalake/book/chapter03/rateCard

for f in dbutils.fs.ls("dbfs:/mnt/datalake/book/chapter03/rateCard"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###4 - Show the contents of the table's transaction log directory

# COMMAND ----------

#%sh
# Run a directory listing of the _delta_log transaction log directory.
# Notice that we have a single transaction entry in ...00000.json
#ls -al /dbfs/mnt/datalake/book/chapter03/rateCard/_delta_log

for f in dbutils.fs.ls("dbfs:/mnt/datalake/book/chapter03/rateCard/_delta_log"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###5 - Show the **metadata** entry in the transaction log entry

# COMMAND ----------

# %sh
# Display the meataData action that was written to the first transaction log entry
# Notice that we first grep for the metaData tag, write the output to a temp file
# and then run the python json.tool on this temp file. This will 'pretty print' 
# our JSON
# grep metadata /dbfs/mnt/datalake/book/chapter03/rateCard/_delta_log/00000000000000000000.json > /tmp/metadata.json
# python -m json.tool /tmp/metadata.json

# COMMAND ----------

# MAGIC %md
# MAGIC ###6 - Created a new Managed table
# MAGIC (potentially remove)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In this CREATE TABLE statement we do NOT specif a location,
# MAGIC -- making it a MANAGED table. This table is managed by hive
# MAGIC -- and it file contents will be stored in the 
# MAGIC -- /user/hive/warehouse/<database name>.db/<table name> directory
# MAGIC CREATE TABLE IF NOT EXISTS taxidb.rateCardManaged
# MAGIC (
# MAGIC     rateCodeId   INT,
# MAGIC     rateCodeDesc STRING
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###7 - Show a directory listing of the managed table
# MAGIC (potentially remove)

# COMMAND ----------

#%sh
# All managed tables will have their file stored under
# the /user/hive/warehouse directory
#ls -al /dbfs/user/hive/warehouse/taxidb.db/ratecardmanaged

for f in dbutils.fs.ls("dbfs:/user/hive/warehouse/taxidb.db/ratecardmanaged"):
    size = f"{f.size:,}".rjust(10)
    typ = "DIR " if f.isDir else "FILE"
    print(f"{typ} {size}  {f.name}")

# COMMAND ----------


