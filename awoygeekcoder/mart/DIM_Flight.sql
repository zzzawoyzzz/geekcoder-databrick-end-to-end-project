-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

-- delete from cleansed_geekcoders.flight where Date_Part='2023-05-01'

-- COMMAND ----------

describe cleansed_geekcoders.flight

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def pre_schema(location):
-- MAGIC     try:
-- MAGIC         df=spark.read.format('delta').load(f"{location}").limit(1)
-- MAGIC         schema=""
-- MAGIC         coloumns=df.dtypes
-- MAGIC         for coloumn in coloumns:
-- MAGIC             schema=schema+f"{coloumn[0]} {coloumn[1]},\n"
-- MAGIC         return(schema[:-2])
-- MAGIC     except Exception as err:
-- MAGIC         print("error occur :",str(err))
-- MAGIC print(pre_schema('/mnt/cleansed_datalake/flight/'))

-- COMMAND ----------

SELECT date,TailNum,ArrDelay,deptime,Origin,Cancelled,CancellationCode,FlightNum,UniqueCarrier
FROM cleansed_geekcoders.flight
WHERE FlightNum=2891
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Reporting_flight (
  date date,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Cancelled int,
  CancellationCode string,
  UniqueCarrier string,
  FlightNum int,
  TailNum int,
  deptime string
) USING DELTA 
PARTITIONED by (date_year int) 
LOCATION '/mnt/datalake_mart/reporting_flight'

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_year=spark.sql("select year(max(date))from cleansed_geekcoders.flight").collect()[0][0]
-- MAGIC print(max_year)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC insert OVERWRITE Reporting_flight partition (date_year ={max_year})
-- MAGIC select
-- MAGIC   date,
-- MAGIC   ArrDelay ,
-- MAGIC   DepDelay ,
-- MAGIC   Origin ,
-- MAGIC   Cancelled ,
-- MAGIC   CancellationCode ,
-- MAGIC   UniqueCarrier ,
-- MAGIC   FlightNum ,
-- MAGIC   TailNum ,
-- MAGIC   deptime 
-- MAGIC FROM
-- MAGIC   cleansed_geekcoders.flight
-- MAGIC Where year(date)={max_year}
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

SELECT *
FROM mart_geekcoders.reporting_flight;

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------


