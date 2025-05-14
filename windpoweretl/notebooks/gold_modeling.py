from pyspark.sql.functions import col, concat_ws, row_number
from pyspark.sql.window import Window

df = spark.table("wind.silver_production_transforme")

# Date dimension
date_dim = df.select(
    col("date").alias("date_id"),
    col("jour").alias("day"),
    col("mois").alias("month"),
    col("trimestre").alias("quarter"),
    col("annee").alias("year")
).distinct()

# Time dimension
df = df.withColumn("time_id", concat_ws(":", col("heure"), col("minute"), col("seconde")))

time_dim = df.select(
    "time_id",
    col("heure").alias("hour_of_day"),
    col("minute").alias("minute_of_hour"),
    col("seconde").alias("second_of_minute")
).distinct()

# Turbine dimension
turbine_dim = df.select(
    "nom_turbine", "capacite", "nom_emplacement", "latitude", "longitude", "region"
).distinct().withColumn(
    "turbine_id", row_number().over(Window.orderBy("nom_turbine", "capacite", "nom_emplacement"))
)

# Status dimension
etat_dim = df.select(
    "etat", "departement_responsable"
).distinct().withColumn(
    "status_id", row_number().over(Window.orderBy("etat", "departement_responsable"))
)

# Fact table
fact_production = df \
    .join(turbine_dim, ["nom_turbine", "capacite", "nom_emplacement", "latitude", "longitude", "region"], "left") \
    .join(etat_dim, ["etat", "departement_responsable"], "left") \
    .select(
        col("id_production"),
        col("date").alias("date_id"),
        "time_id",
        "turbine_id",
        "status_id",
        "vitesse_vent",
        "direction_vent",
        "energie_produite"
    )

# Save all
spark.sql("CREATE SCHEMA IF NOT EXISTS wind")
date_dim.write.format("delta").mode("overwrite").saveAsTable("wind.dim_date")
time_dim.write.format("delta").mode("overwrite").saveAsTable("wind.dim_temps")
turbine_dim.write.format("delta").mode("overwrite").saveAsTable("wind.dim_turbine")
etat_dim.write.format("delta").mode("overwrite").saveAsTable("wind.dim_etat")
fact_production.write.format("delta").mode("overwrite").saveAsTable("wind.fact_production")
