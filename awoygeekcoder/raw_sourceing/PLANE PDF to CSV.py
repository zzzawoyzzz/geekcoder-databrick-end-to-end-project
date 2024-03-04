# Databricks notebook source
# !pip uninstall tabula-py -y

# COMMAND ----------

!pip install tabula-py==2.7.0

# COMMAND ----------

!pip install tabula-py

# COMMAND ----------

# dbutils.fs.mkdirs(f'/dbfs/mnt/raw_datalake/PLAIN/DatePart={date.today()}/PLAIN.csv')

# COMMAND ----------

output_dir = '/dbfs/mnt/raw_datalake/PLAIN/DatePart=2024-02-05/'
display(dbutils.fs.ls('/dbfs/mnt/raw_datalake/PLAIN/'))

# COMMAND ----------

import tabula
from datetime import date
print(date.today())
today=str(date.today())
output_dir = f'/dbfs/mnt/raw_datalake/PLAIN/Datepart={today}'
# dbutils.fs.mkdirs(output_dir)
tabula.convert_into(
    '/dbfs/mnt/source_blob/PLANE.pdf',
    f'{output_dir}'+"/PLAIN.csv",
    output_format='csv',
    pages='all'
)


# COMMAND ----------

# import tabula
# from datetime import date
# from pyspark.sql import SparkSession

# # Initialize Spark session
# spark = SparkSession.builder.getOrCreate()

# today = str(date.today())

# output_dir = f'/mnt/raw_datalake/PLAIN/DatePart={today}'

# # Use dbutils.fs.mkdirs for creating directories
# dbutils.fs.mkdirs(output_dir)

# # Use Spark session to read the PDF
# pdf_path = '/mnt/source_blob/PLANE.pdf'
# df = spark.read.format("pdf").option("multiline", "true").load(pdf_path)

# # Convert the DataFrame to CSV
# df.write.mode("overwrite").option("header", "true").csv(output_dir)

# # Move the CSV files to the correct directory
# csv_files = output_dir + "/PLAIN.csv"
# dbutils.fs.mv(csv_files, output_dir)


# COMMAND ----------

list_files=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if (i.name.split('.'))[1]=='pdf']
print(list_files)

# COMMAND ----------

import tabula
from datetime import date

def f_source_pdf_datalake (source_path,sink_path,output_format,page,flie_name):
    try :
        today=str(date.today())
        dbutils.fs.mkdirs(f"/{sink_path}{flie_name.split('.')[0]}/DatePart={today}/")
        tabula.convert_into(
            # source file
            f"{source_path}{flie_name}",
            #sink file
            f"/dbfs/{sink_path}{flie_name.split('.')[0]}/DatePart={today}/{flie_name.split('.')[0]}.{output_format}",
            output_format=output_format,
            pages=page
        )
    except Exception as err:
        print("error occur :",str(err))


# COMMAND ----------

list_files=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_blob/') if (i.name.split('.'))[1]=='pdf']
for i in list_files:
    f_source_pdf_datalake(source_path='/dbfs/mnt/source_blob/',sink_path='mnt/raw_datalake/',output_format='csv',page='all',flie_name=i[0])
