# Databricks notebook source
# MAGIC %md create mount points automatically with functions

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get(scope="formula1-scope", key="client-id")
    tenant_id = dbutils.secrets.get(scope="formula1-scope", key="tenant-id")
    client_secret = dbutils.secrets.get(scope="formula1-scope", key="client-secret")

    # set spark config
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # Unmount the directory if it is already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # mount the storage account container
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('containerformula1','raw')

# COMMAND ----------

mount_adls('containerformula1','presentation')

# COMMAND ----------

mount_adls('containerformula1','processed')

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key = "client-secret")

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@containerformula1.dfs.core.windows.net/",
  mount_point = "/mnt/containerformula1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@containerformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@containerformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

