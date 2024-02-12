# Databricks notebook source
# MAGIC %run /awoygeekcoder/utility

# COMMAND ----------

display(dbutils.fs.ls('/mnt/FileStore/tables/schema/Flight'))

# COMMAND ----------

df=spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemalocation","/mnt/FileStore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/flight/')


# COMMAND ----------

display(dbutils.fs.ls('/dbfs/FileStore/tables/schema/Flight'))
display(dbutils.fs.ls('/mnt/cleansed_datalake/flight/'))
display(dbutils.fs.ls('/mnt/FileStore/tables/schema/Flight'))

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/schema/Flight',True)
dbutils.fs.rm('/mnt/cleansed_datalake/flight/',True)
# dbutils.fs.rm('/mnt/FileStore/tables/schema/Flight',True)


# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

display(df.selectExpr(
"to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as deptime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as ArrTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSArrTime",
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum" ,
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(AirTime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
 "Origin",
 "Dest",
 "cast(Distance as int) as  Distance",
 "cast(TaxiIn as int) as TaxiIn",
 "cast(TaxiOut as int) as TaxiOut",
 "Cancelled",
 "CancellationCode",
 "cast(Diverted as int) as castDiverted",
 "cast(CarrierDelay as int) as CarrierDelay",
 "cast(WeatherDelay as int) as WeatherDelay" ,
 "cast(NASDelay as int) as NASDelay",
 "cast(SecurityDelay as int) as SecurityDelay",
 "cast(LateAircraftDelay as int) as LateAircraftDelay" ,
 "to_date(Date_Part,'yyyy-MM-dd') as Date_Part "
))



# COMMAND ----------

from pyspark.sql.functions import col,to_date,concat_ws,from_unixtime,unix_timestamp,when
df_base=df.selectExpr(
"to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as deptime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as ArrTime",
"from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm')  as CRSArrTime",
"UniqueCarrier",
"cast(FlightNum as int) as FlightNum",
"cast(TailNum as int) as TailNum" ,
"cast(ActualElapsedTime as int) as ActualElapsedTime",
"cast(CRSElapsedTime as int) as CRSElapsedTime",
"cast(AirTime as int) as AirTime",
"cast(ArrDelay as int) as ArrDelay",
"cast(DepDelay as int) as DepDelay",
 "Origin",
 "Dest",
 "cast(Distance as int) as  Distance",
 "cast(TaxiIn as int) as TaxiIn",
 "cast(TaxiOut as int) as TaxiOut",
 "Cancelled",
 "CancellationCode",
 "cast(Diverted as int) as castDiverted",
 "cast(CarrierDelay as int) as CarrierDelay",
 "cast(WeatherDelay as int) as WeatherDelay" ,
 "cast(NASDelay as int) as NASDelay",
 "cast(SecurityDelay as int) as SecurityDelay",
 "cast(LateAircraftDelay as int) as LateAircraftDelay" ,
 "to_date(Date_Part,'yyyy-MM-dd') as Date_Part "
)
display(df_base)
df_base.writeStream.trigger(once=True)\
        .format('delta')\
        .option("checkpointLocation",'/dbfs/FileStore/tables/schema/Flight')\
        .start('/mnt/cleansed_datalake/flight/')

# COMMAND ----------

# from pyspark.sql.functions import col,to_date,concat_ws,from_unixtime,unix_timestamp,when
# df_base=df.select(
#         to_date(concat_ws('-',col("Year"),col("Month"),col("DayofMonth")),'yyyy-MM-dd').alias("date"),\
#         from_unixtime(unix_timestamp(when(col("DepTime") == 2400, 0).otherwise(col("DepTime")), 'HH:mm')).alias("deptime"),
#         from_unixtime(unix_timestamp(when(col("CRSDepTime") == 2400, 0).otherwise(col("CRSDepTime")), 'HH:mm')).alias("CRSDepTime"),
#         from_unixtime(unix_timestamp(when(col("ArrDelay") == 2400, 0).otherwise(col("ArrDelay")), 'HH:mm')).alias("ArrDelay"),
#         from_unixtime(unix_timestamp(when(col("CRSArrTime") == 2400, 0).otherwise(col("CRSArrTime")), 'HH:mm')).alias("CRSArrTime"),\
#         col("UniqueCarrier"),\
#         col("FlightNum").cast("int"),\
#         col("TailNum"),\
#         col("ActualElapsedTime").cast("int"),\
#         col("CRSElapsedTime").cast("int"),\
#         col("AirTime").cast("int"),\
#         col("DepDelay"),\
#         col("Origin"),        
#         col("Dest"),\
#         col("Distance"),\
#         col("TaxiIn").cast("int"),\
#         col("TaxiOut").cast("int"),\
#         col("Cancelled").cast("int"),\
#         col("CancellationCode"),\
#         col("Diverted").cast("int"),\
#         col("CarrierDelay").cast("int"),\
#         col("WeatherDelay").cast("int"),\
#         col("NASDelay").cast("int"),         
#         col("SecurityDelay").cast("int"),\
#         col("LateAircraftDelay").cast("int"),\
#         to_date(col("Date_part"),"yyyy-MM-dd").alias('Date_part'),\
#           )
# display(df_base)
# df_base.writeStream.trigger(once=True)\
#     .format("delta")\
#     .option("checkpointLocation","/dbfs/FileStore/tables/schema/Flight")\
#     .start("/mnt/cleansed_datalake/flight/")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/cleansed_datalake/flight'))

# COMMAND ----------


f_delta_cleansed_load(table_name='flight',location="/mnt/cleansed_datalake/flight/",database="cleansed_geekcoders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_geekcoders.flight;
