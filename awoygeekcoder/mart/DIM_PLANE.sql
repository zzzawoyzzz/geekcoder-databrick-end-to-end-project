-- Databricks notebook source
use mart_geekcoders;

-- COMMAND ----------

use cleansed_geekcoders;
show tables;

-- COMMAND ----------

delete from cleansed_geekcoders.PLANE where Date_Part='2023-05-01'

-- COMMAND ----------

select COUNT(*),COUNT(distinct tailid) from cleansed_geekcoders.plane;

-- COMMAND ----------

describe cleansed_geekcoders.plane

-- COMMAND ----------

show databases;
use mart_geekcoders;
show tables;

-- COMMAND ----------

use mart_geekcoders;
CREATE TABLE IF NOT EXISTS DIM_PLANE (
  tailid STRING,
  type STRING,
  manufacturer STRING,
  issue_date STRING,
  model STRING,
  status STRING,
  aircraft_type STRING,
  engine_type STRING,
  year int,
  Date_Part date
) USING DELTA LOCATION '/mnt/datalake_mart/DIM_PLANE'

-- COMMAND ----------

insert overwrite DIM_PLANE
select tailid ,
type ,
manufacturer ,
issue_date ,
model ,
status ,
aircraft_type ,
engine_type ,
year,
Date_Part
FROM cleansed_geekcoders.plane

-- COMMAND ----------

select *
from mart_geekcoders.dim_plane

-- COMMAND ----------


