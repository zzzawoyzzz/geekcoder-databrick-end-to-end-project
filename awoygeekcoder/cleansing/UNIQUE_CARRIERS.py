# Databricks notebook source
# MAGIC %run /awoygeekcoder/utility

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemalocation","/dbfs/mnt/FileStore/tables/schema/UNIQUE_CARRIERS")\
    .load('/mnt/raw_datalake/UNIQUE_CARRIERS/')


# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/schema/Cancellation',True)
# dbutils.fs.rm('/mnt/cleansed_datalake/cancellation/',True)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.selectExpr("replace(Code,'\"','') as code",
                    "replace(Description,'\"','') as description",
                    "to_date(DateTime,'yyyy-MM-dd') as Date_Part") 
        )

# COMMAND ----------

df_base=df.selectExpr("replace(Code,'\"','') as code",
                    "replace(Description,'\"','') as description",
                    "to_date(DateTime,'yyyy-MM-dd') as Date_Part",
                      )
display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")\
    .start("/mnt/cleansed_datalake/UNIQUE_CARRIERS/")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/cleansed_datalake/UNIQUE_CARRIERS/'))

# COMMAND ----------

f_delta_cleansed_load(table_name='UNIQUE_CARRIERS',location="/mnt/cleansed_datalake/UNIQUE_CARRIERS/",database="cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.UNIQUE_CARRIERS;

# COMMAND ----------


