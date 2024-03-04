-- Databricks notebook source
use cleansed_geekcoders;
show tables;

-- COMMAND ----------

describe cleansed_geekcoders.airport

-- COMMAND ----------

SELECT * 
FROM cleansed_geekcoders.airport

-- COMMAND ----------

use mart_geekcoders;
CREATE TABLE IF NOT EXISTS Dim_Airport (
  code string,
  city string,
  country string,
  airport string
) 
USING DELTA 
LOCATION '/mnt/datalake_mart/Dim_Airport';

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

SELECT * 
FROM mart_geekcoders.dim_airport

-- COMMAND ----------

insert
  overwrite Dim_Airport
select
  code string,
  city string,
  country string,
  airport string
FROM
  cleansed_geekcoders.airport

-- COMMAND ----------


