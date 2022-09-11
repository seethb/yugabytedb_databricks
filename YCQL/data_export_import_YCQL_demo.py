# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will help you to learn how YugabyteDB(YCQL) can connect from Azure Databricks. 
# MAGIC In this demo, following scenarios are covered
# MAGIC 
# MAGIC 1. Load CSV from Databricks Filestore into YugabyteDB 
# MAGIC 2. Import Data into YugabyteDB
# MAGIC    2a. Import Parquet file into YugabyteDB
# MAGIC    2b. Import Avro file into YugabyteDB
# MAGIC 3. Export YugabyteDB Table into Storage Folder
# MAGIC    3a.  Export YugabyteDB table into Parquet file
# MAGIC    3b.  Export YugabyteDB table into Avro file
# MAGIC 4. Import Delta Table into YugabyteDB table  
# MAGIC 5. Export YugabyteDB table into Delta Lake Table
# MAGIC 6. Data Visualization and sample queries

# COMMAND ----------

# MAGIC %python
# MAGIC #1. Load Salesrecords CSV directly into YugabyteDB (YCQL) Table
# MAGIC df1 = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("dbfs:/FileStore/tables/1000_Sales_Records_1.csv")
# MAGIC 
# MAGIC import os
# MAGIC import pyspark
# MAGIC #import pyspark_cassandra
# MAGIC import pandas as pd
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql import SQLContext
# MAGIC from pyspark import SparkContext
# MAGIC from pyspark.sql.functions import col
# MAGIC spark = (SparkSession.builder
# MAGIC     .appName('simple_etl')
# MAGIC     .config("spark.cassandra.connection.host", "10.14.16.7")
# MAGIC     .config('spark.cassandra.connection.port', '9042')
# MAGIC     .config("spark.cassandra.auth.username", "cassandra")
# MAGIC     .config("spark.cassandra.auth.password", "cassandra")
# MAGIC     .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
# MAGIC     .config("spark.cassandra.connection.localDC", "eastus")
# MAGIC     .getOrCreate())
# MAGIC df1.createOrReplaceTempView("salesorder")
# MAGIC pqrdf = spark.sql("SELECT orderid, region,country,itemtype,saleschannel,orderpriority,orderdate,shipdate,unitssold, unitprice, unitcost, totalrevenue, totalcost , totalprofit  FROM salesorder ")
# MAGIC pqrdf.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="salesprofit_test", keyspace ="demo").save()
# MAGIC #df1.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="salesprofit_test1", keyspace="demo").save()

# COMMAND ----------

# MAGIC %python
# MAGIC #2a. Read parquet file and load into YugabyteDB
# MAGIC sourcedf = sqlContext.read.parquet("/tmp/salesprofit")
# MAGIC sourcedf.createOrReplaceTempView("salesorder")
# MAGIC df = spark.sql("SELECT orderid, region,country,itemtype,saleschannel,orderpriority,orderdate,shipdate,unitssold, unitprice, unitcost, totalrevenue, totalcost , totalprofit  FROM salesorder ")
# MAGIC df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="salesprofit_test", keyspace ="demo").save()

# COMMAND ----------

# MAGIC %python
# MAGIC #2b. Read avro file and load into YugabyteDB
# MAGIC datafavro = spark.read.format("avro").load("/tmp/salesprofit_avro_export")
# MAGIC datafavro.createOrReplaceTempView("salesorder")
# MAGIC df = spark.sql("SELECT orderid, region,country,itemtype,saleschannel,orderpriority,orderdate,shipdate,unitssold, unitprice, unitcost, totalrevenue, totalcost , totalprofit  FROM salesorder ")
# MAGIC df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="salesprofit_avro_test", keyspace ="demo").save()

# COMMAND ----------

# MAGIC %python
# MAGIC #3a. Export YugabyteDB table into parquet file format.
# MAGIC pd = spark.read.format("org.apache.spark.sql.cassandra").options(table="salesprofit_test", keyspace="demo").load()
# MAGIC pd.write.mode("overwrite").parquet("/tmp/export/salesprofit_test")

# COMMAND ----------

# MAGIC %python
# MAGIC #3a. Export YugabyteDB table into avro file format
# MAGIC pd = spark.read.format("org.apache.spark.sql.cassandra").options(table="salesprofit_avro_test", keyspace="demo").load()
# MAGIC pd.write.format('avro').mode('overwrite').save("/tmp/export/avro/salesprofit_test_avro")

# COMMAND ----------

# MAGIC %python
# MAGIC #4 Import data from Delta table to YugabyteDB (YCQL)
# MAGIC dataf = spark.read.format("delta").load("dbfs:/user/hive/warehouse/sample_table4")
# MAGIC dataf.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate","true").options(table="employee", keyspace ="demo").save()

# COMMAND ----------

# MAGIC %python
# MAGIC #5 Read a table from YugabyteDB (YCQL) using JBDC and load into Delta table
# MAGIC pd = spark.read.format("org.apache.spark.sql.cassandra").options(table="salesprofit_test", keyspace="demo").load()
# MAGIC pd.write.format ( "delta" ).mode("overwrite").saveAsTable ( "sales_details" )

# COMMAND ----------

# MAGIC %python
# MAGIC #6. Data Visualization using YugabyteDB
# MAGIC df = spark.read.format("org.apache.spark.sql.cassandra").options(table="salesprofit_test", keyspace="demo").load()
# MAGIC df.show()
# MAGIC df.createOrReplaceTempView ( "profitanalysis" )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM profitanalysis 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, sum(TotalRevenue) FROM profitanalysis group by region
