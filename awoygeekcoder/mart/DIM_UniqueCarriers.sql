-- Databricks notebook source
use cleansed_geekcoders;
show tables;

-- COMMAND ----------

describe cleansed_geekcoders.unique_carriers

-- COMMAND ----------

SELECT * 
FROM cleansed_geekcoders.unique_carriers

-- COMMAND ----------

use mart_geekcoders;
CREATE TABLE IF NOT EXISTS Dim_unique_carriers (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/datalake_mart/Dim_Unique_Carriers';

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

SELECT * 
FROM mart_geekcoders.dim_unique_carriers

-- COMMAND ----------

insert
  overwrite Dim_unique_carriers
select
  code STRING,
  description STRING
FROM
  cleansed_geekcoders.unique_carriers

-- COMMAND ----------


