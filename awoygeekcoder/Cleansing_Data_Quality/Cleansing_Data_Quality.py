# Databricks notebook source
# MAGIC %run /Workspace/awoygeekcoder/utility

# COMMAND ----------

list_table_info=[
    ("STREAMING UPDATE",'airport',100),
    ("STREAMING UPDATE",'plane',100),
    ("STREAMING UPDATE",'flight',100),
    ("STREAMING UPDATE",'cancellation',100),
    ("STREAMING UPDATE",'UNIQUE_CARRIERS',100)
]
for table_info in list_table_info:
    f_count_check('cleansed_geekcoders',table_info[0],table_info[1],table_info[2])


# COMMAND ----------


