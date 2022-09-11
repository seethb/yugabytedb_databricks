// Databricks notebook source
// MAGIC %md
// MAGIC This notebook will help you to learn how YugabyteDB(YSQL) can connect from Azure Databricks. 
// MAGIC In this demo, following scenarios are covered
// MAGIC 
// MAGIC 1. Load CSV from Databricks Filestore into YugabyteDB 
// MAGIC 2. Import Data into YugabyteDB
// MAGIC    2a. Import Parquet file into YugabyteDB
// MAGIC    2b. Import Avro file into YugabyteDB
// MAGIC 3. Export YugabyteDB Table into Storage Folder
// MAGIC    3a.  Export YugabyteDB table into Parquet file
// MAGIC    3b.  Export YugabyteDB table into Avro file
// MAGIC 4. Import Delta Table into YugabyteDB table  
// MAGIC 5. Export YugabyteDB table into Delta Lake Table
// MAGIC 6. Data Visualization and sample queries

// COMMAND ----------

// MAGIC %python
// MAGIC #1. Load Salesrecords CSV directly into YugabyteDB Table
// MAGIC df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/1000_Sales_Records_1.csv")
// MAGIC df1.write.format("jdbc")\
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.salesprofit_details") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .mode("overwrite").save()

// COMMAND ----------

// MAGIC %python
// MAGIC #2a. Read parquet file and load into YugabyteDB
// MAGIC sourcedf = sqlContext.read.parquet("/tmp/salesprofit")
// MAGIC sourcedf.show()
// MAGIC sourcedf.write.format("jdbc")\
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.salesprofit_details") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .mode("overwrite").save()

// COMMAND ----------

// MAGIC %python
// MAGIC #2b. Read avro file and load into YugabyteDB
// MAGIC datafavro = spark.read.format("avro").load("/tmp/salesprofit_avro_export")
// MAGIC datafavro.show()
// MAGIC datafavro.write.format("jdbc")\
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.salesprofit_details_avro") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .mode("overwrite").save()

// COMMAND ----------

// MAGIC %python
// MAGIC #3a. Export YugabyteDB table into parquet file format.
// MAGIC df= spark.read \
// MAGIC         .format("jdbc") \
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.product_master") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .load()
// MAGIC 
// MAGIC df.write.mode("overwrite").parquet("/tmp/product_master")

// COMMAND ----------

// MAGIC %python
// MAGIC #3a. Export YugabyteDB table into avro file format
// MAGIC df= spark.read \
// MAGIC         .format("jdbc") \
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.product_master") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .load()
// MAGIC 
// MAGIC df.write.format('avro').mode('overwrite').save("/tmp/product_master_avro")

// COMMAND ----------

// MAGIC %python
// MAGIC #4 Import data from Delta table to YugabyteDB (YSQL)
// MAGIC dataf = spark.read.format("delta").load("dbfs:/user/hive/warehouse/sample_table4")
// MAGIC dataf.show()
// MAGIC dataf.write.format("jdbc")\
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.testdelta") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .mode("overwrite").save()

// COMMAND ----------

// MAGIC %python
// MAGIC #5 Read a table from YugabyteDB (YSQL) using JBDC and load into Delta table
// MAGIC jdbcDF = spark.read \
// MAGIC         .format("jdbc") \
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.employee") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .load()
// MAGIC jdbcDF.write.format ( "delta" ).mode("overwrite").saveAsTable ( "SAMPLE_TABLE5" )

// COMMAND ----------

// MAGIC %python
// MAGIC #6. Data Visualization using YugabyteDB
// MAGIC 
// MAGIC df= spark.read \
// MAGIC         .format("jdbc") \
// MAGIC         .option("url", "jdbc:postgresql://10.14.16.7:5433/yugabyte") \
// MAGIC         .option("dbtable", "public.salesprofit_details") \
// MAGIC         .option("user", "yugabyte") \
// MAGIC         .option("driver", "org.postgresql.Driver") \
// MAGIC         .option("password", "yugabyte") \
// MAGIC         .option("useSSL", "true") \
// MAGIC         .option("ssl", "true") \
// MAGIC         .option("sslmode", "require") \
// MAGIC         .load()
// MAGIC df.show()
// MAGIC df.createOrReplaceTempView ( "profitanalysis" )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM profitanalysis 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT region, sum(TotalRevenue) FROM profitanalysis group by region
