
This notebook will help you to learn how YugabyteDB(YCQL) can connect from Azure Databricks. In this demo, following scenarios are covered

1. Load CSV from Databricks Filestore into YugabyteDB
2. Import Data into YugabyteDB 
2a. Import Parquet file into YugabyteDB 
2b. Import Avro file into YugabyteDB
3. Export YugabyteDB Table into Storage Folder 
3a. Export YugabyteDB table into Parquet file 
3b. Export YugabyteDB table into Avro file
4. Import Delta Table into YugabyteDB table
5. Export YugabyteDB table into Delta Lake Table
6. Data Visualization and sample queries


Note: Before running the above scenarios. the sample code expects the following keyspace and table details loaded in YCQL

create keyspace demo; 
create table demo.employee(id integer primary key, name text, age text, language text);
create table demo.salesprofit_test(orderid text primary key, region text,country text,itemtype text,saleschannel text,orderpriority text,orderdate text,shipdate text,unitssold  text, unitprice  text, unitcost  text, totalrevenue  text, totalcost text, totalprofit  text);
create table demo.salesprofit_test1(orderid text primary key, region text,country text,itemtype text,saleschannel text,orderpriority text,orderdate text,shipdate text,unitssold  text, unitprice  text, unitcost  text, totalrevenue  text, totalcost text, totalprofit  text);
