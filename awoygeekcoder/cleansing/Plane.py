# Databricks notebook source
# MAGIC %run /awoygeekcoder/utility

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemalocation","/dbfs/mnt/FileStore/tables/schema/plane")\
    .load('/mnt/raw_datalake/PLANE/')


# COMMAND ----------

# dbutils.fs.rm('/mnt/cleansed_datalake/plane/',True)

# COMMAND ----------

# display(dbutils.fs.ls('/mnt/cleansed_datalake/plane/'))

# COMMAND ----------

df_base=df.selectExpr("tailnum as tailid","type","manufacturer","to_date(issue_date) AS issue_date","model","status","aircraft_type","engine_type","cast(year as int) as year","to_date(DatePart,'yyyy-MM-dd') as Date_Part"
                      )
display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/schema/PLANE")\
    .start("/mnt/cleansed_datalake/plane/")

# COMMAND ----------

f_delta_cleansed_load(table_name='plane',location="/mnt/cleansed_datalake/plane/",database="cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM cleansed_geekcoders.plane
# MAGIC ;

# COMMAND ----------


