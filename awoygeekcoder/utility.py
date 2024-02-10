# Databricks notebook source

def pre_schema(location):
    try:
        df=spark.read.format('delta').load(f"{location}").limit(1)
        schema=""
        coloumns=df.dtypes
        for coloumn in coloumns:
            schema=schema+f"{coloumn[0]} {coloumn[1]},\n"
        return(schema[:-2])
    except Exception as err:
        print("error occur :",str(err))


# COMMAND ----------

def f_delta_cleansed_load(table_name, location, database):
    try:
        schema = pre_schema(f"{location}")
        spark.sql(f""" DROP TABLE IF EXISTS {database}.{table_name}""")
        spark.sql(
            f"""
              CREATE TABLE IF NOT EXISTS {database}.{table_name}
              (
              {schema}
              )
              USING delta
              LOCATION '{location}'
              """.format(
                schema, location, table_name, database
            )
        )
    except Exception as err:
        print("error from f_delta_cleansed_load occur :", str(err))

# COMMAND ----------

def f_count_check(database,operation_type,table_name,number_diff):

    spark.sql(f"""desc history {database}.{table_name}""").createOrReplaceTempView("Table_count")
    count_current = spark.sql(
    f"""SELECT operationMetrics.numOutputRows
    From Table_count
    WHERE version = (select MAX(version)
                    FROM Table_count
                    WHERE trim(lower(operation))= lower('{operation_type}'))
                    """
    )
    if count_current.first() is None:
        final_count_current =0
    else:
        final_count_current=int(count_current.first().numOutputRows)

    count_previuos = spark.sql(
        f"""
    SELECT operationMetrics.numOutputRows
    From Table_count
    WHERE version < (select version
                    FROM Table_count
                    WHERE trim(lower(operation))= lower('{operation_type}')
                    ORDER BY version desc
                    LIMIT 1 )
                    """
    )
    if count_previuos.first() is None:
        final_count_previous =0
    else:
        final_count_previous=int(count_previuos.first().numOutputRows)


    if (final_count_current-final_count_previous)>number_diff:
        raise Exception("Difference is huge ",table_name)
    else:
        pass

# COMMAND ----------


