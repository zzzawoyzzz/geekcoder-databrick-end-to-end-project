-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

use cleansed_geekcoders;
show tables;

-- COMMAND ----------

describe cleansed_geekcoders.cancellation

-- COMMAND ----------

SELECT * 
FROM cleansed_geekcoders.cancellation

-- COMMAND ----------

use mart_geekcoders;
CREATE TABLE IF NOT EXISTS Dim_Cancellation (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/datalake_mart/Dim_Cancelation';

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

SELECT * 
FROM mart_geekcoders.Dim_Cancellation

-- COMMAND ----------

insert
  overwrite Dim_Cancellation
select
  code STRING,
  description STRING
FROM
  cleansed_geekcoders.cancellation

-- COMMAND ----------


