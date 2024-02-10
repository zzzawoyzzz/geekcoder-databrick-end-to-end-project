# Databricks notebook source
# MAGIC %run /awoygeekcoder/utility

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemalocation","/dbfs/mnt/FileStore/tables/schema/Airport")\
    .load('/mnt/raw_datalake/Airport/')


# COMMAND ----------

df_base=df.selectExpr("Code as code",
                      "split(Description,',')[0] as city",
                      "split(split(Description,',')[0],':')[0] AS country",
                      "split(split(Description,',')[1],':')[1] AS airport",
                      "to_date(Date_Part,'yyyy-MM-dd') as Date_Part",
                      )
display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/schema/Airport")\
    .start("/mnt/cleansed_datalake/airport/")

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/cleansed_datalake/airport/')

# COMMAND ----------

# dbutils.fs.rm('dbfs:/dbfs/FileStore/tables/schema/airport/',True)
# dbutils.fs.rm('/mnt/cleansed_datalake/airport/',True)

# COMMAND ----------

display(dbutils.fs.ls('/dbfs/FileStore/tables/schema/'))

# COMMAND ----------

f_delta_cleansed_load(table_name='airport',location="/mnt/cleansed_datalake/airport/",database="cleansed_geekcoders",schema='')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.airport;

# COMMAND ----------


