# Databricks notebook source
# MAGIC %md steps to follow
# MAGIC 1. Register Azure AD application/service principal
# MAGIC 2. generate a secret/password for the application
# MAGIC 3. set spark config with app/client ID, directory/tenant ID & secret
# MAGIC 4. Assign role "Storage Blob data contributor" to the data lake

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list(scope="formula1-scope"))

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key = "client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key = "tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key = "client-secret")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC spark.conf.set("fs.azure.account.auth.type.containerformula1.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.containerformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.containerformula1.dfs.core.windows.net", client_id)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.containerformula1.dfs.core.windows.net", client_secret)
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.containerformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

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

