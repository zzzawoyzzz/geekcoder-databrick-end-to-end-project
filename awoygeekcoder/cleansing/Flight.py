# Databricks notebook source
# MAGIC %run /awoygeekcoder/utility

# COMMAND ----------

display(dbutils.fs.ls('/mnt/raw_datalake/'))

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemalocation","/dbfs/mnt/FileStore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/flight/')


# COMMAND ----------

# display(dbutils.fs.ls('/dbfs/FileStore/tables/schema/Flight'))
# display(dbutils.fs.ls('/mnt/cleansed_datalake/flight/'))

# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/schema/Cancellation',True)
# dbutils.fs.rm('/mnt/cleansed_datalake/cancellation/',True)

# COMMAND ----------

display(df)

# COMMAND ----------

# display(df.selectExpr("replace(Code,'\"','') as code",
#                     "replace(Description,'\"','') as description",
#                     "to_date(DateTime,'yyyy-MM-dd') as Date_Part") 
#         )
from pyspark.sql.functions import col,to_date
df.select(col("Year").cast("int"),\
        col("Month").cast("int"),\
        col("DayofMonth").cast("int"),\
        col("DayOfWeek").cast("int"),\
        col("DepTime").cast("int"),\
        col("CRSDepTime").cast("int"),\
        col("CRSArrTime").cast("int"),\
        col("UniqueCarrier"),\
        col("FlightNum").cast("int"),\
        col("TailNum"),\
        col("ActualElapsedTime").cast("int"),\
        col("CRSElapsedTime").cast("int"),\
        col("AirTime"),\
        col("ArrDelay"),   
        col("DepDelay"),\
        col("Origin"),        
        col("Dest"),\
        col("Distance"),\
        col("TaxiIn").cast("int"),\
        col("TaxiOut").cast("int"),\
        col("Cancelled").cast("int"),\
        col("CancellationCode"),\
        col("Diverted").cast("int"),\
        col("CarrierDelay").cast("int"),\
        col("WeatherDelay").cast("int"),\
        col("NASDelay").cast("int"),         
        col("SecurityDelay").cast("int"),\
        col("LateAircraftDelay").cast("int"),\
        to_date(col("Date_part"),"yyyy-MM-dd").alias('Date_part'),\
          )

# COMMAND ----------

df_base=df.select(col("Year").cast("int"),\
        col("Month").cast("int"),\
        col("DayofMonth").cast("int"),\
        col("DayOfWeek").cast("int"),\
        col("DepTime").cast("int"),\
        col("CRSDepTime").cast("int"),\
        col("CRSArrTime").cast("int"),\
        col("UniqueCarrier"),\
        col("FlightNum").cast("int"),\
        col("TailNum"),\
        col("ActualElapsedTime").cast("int"),\
        col("CRSElapsedTime").cast("int"),\
        col("AirTime"),\
        col("ArrDelay"),   
        col("DepDelay"),\
        col("Origin"),        
        col("Dest"),\
        col("Distance"),\
        col("TaxiIn").cast("int"),\
        col("TaxiOut").cast("int"),\
        col("Cancelled").cast("int"),\
        col("CancellationCode"),\
        col("Diverted").cast("int"),\
        col("CarrierDelay").cast("int"),\
        col("WeatherDelay").cast("int"),\
        col("NASDelay").cast("int"),         
        col("SecurityDelay").cast("int"),\
        col("LateAircraftDelay").cast("int"),\
        to_date(col("Date_part"),"yyyy-MM-dd").alias('Date_part'),\
          )
display(df_base)
df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/schema/Flight")\
    .start("/mnt/cleansed_datalake/flight/")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/cleansed_datalake/flight'))

# COMMAND ----------


f_delta_cleansed_load(table_name='flight',location="/mnt/cleansed_datalake/flight/",database="cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.flight;

# COMMAND ----------


