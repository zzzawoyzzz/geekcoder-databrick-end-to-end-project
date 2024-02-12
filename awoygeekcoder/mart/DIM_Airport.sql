-- Databricks notebook source
use cleansed_geekcoders;
show tables;

-- COMMAND ----------

describe cleansed_geekcoders.airline

-- COMMAND ----------

SELECT * 
FROM cleansed_geekcoders.airline

-- COMMAND ----------

use mart_geekcoders;
CREATE TABLE IF NOT EXISTS Dim_Airlines (
  iata_code STRING,
  icao_code STRING,
  name STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_Airlines'

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

SELECT * 
FROM mart_geekcoders.dim_Airline

-- COMMAND ----------

INSERT
  OVERWRITE Dim_Airlines
SELECT
  iata_code STRING,
  icao_code STRING,
  name STRING
FROM
  cleansed_geekcoders.Airline

-- COMMAND ----------


