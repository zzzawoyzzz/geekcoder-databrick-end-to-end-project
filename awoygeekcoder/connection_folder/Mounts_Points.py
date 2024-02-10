# Databricks notebook source
# awoy-keyvault-secret

# COMMAND ----------

containerName=dbutils.secrets.get(scope="awoy-keyvault-secret",key="containerName")
storageAccountName=dbutils.secrets.get(scope="awoy-keyvault-secret",key="storageAccountName")
sas=dbutils.secrets.get(scope="awoy-keyvault-secret",key="sas")
config={"fs.azure.sas."+containerName+"."+storageAccountName+".blob.core.windows.net": sas}

dbutils.fs.mount(
    source="wasbs://{}@{}.blob.core.windows.net".format(containerName,storageAccountName),
    mount_point="/mnt/source_blob/",
    extra_configs=config
)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-app-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-app-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-client-refresh-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint = "/mnt/raw_datalake"

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = dbutils.secrets.get(scope="awoy-keyvault-secret",key="datalake-raw"),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = configs
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-app-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-app-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="awoy-keyvault-secret",key="data-client-refresh-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
mountPoint = "/mnt/cleansed_datalake"

if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = dbutils.secrets.get(scope="awoy-keyvault-secret",key="datalake-cleansed"),
      mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = configs
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# abfss://cleansed@awoygeekdatalakegen2.dfs.core.windows.net/
# wasbs://raw@awoygeekdatalakegen2.blob.core.windows.net/

# COMMAND ----------

dbutils.fs.ls("/mnt/source_blob/")

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/source_blob/")
