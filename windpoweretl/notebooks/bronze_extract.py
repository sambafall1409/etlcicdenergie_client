# Configuration ADLS Gen2 - Bronze
storage_account_name = "sambarg"
container_name = "datawindpower"
file_path = "data.csv"
access_key = "wAECvXOvFRxtOUBECl6wSnNqgPJmn1SdYX+MIRbYXNOBYwKsJ3AsjV0Dko6Wuxkt3yUlebMVBd6L+AStcxubTQ=="

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)

# Lecture brut
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_path}"
df_bronze = spark.read.option("header", True).option("inferSchema", True).csv(adls_path)

spark.sql("CREATE SCHEMA IF NOT EXISTS wind")
df_bronze.write.format("delta").mode("overwrite").saveAsTable("wind.silver_production")
