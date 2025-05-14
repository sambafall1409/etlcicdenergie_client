from pyspark.sql.functions import col, regexp_replace, dayofmonth, month, quarter, year, substring

df = spark.table("wind.silver_production")

df_transforme = (
    df.withColumn("jour", dayofmonth(col("date")))
      .withColumn("mois", month(col("date")))
      .withColumn("trimestre", quarter(col("date")))
      .withColumn("annee", year(col("date")))
      .withColumn("heure_texte", regexp_replace(col("time"), "-", ":"))
      .withColumn("heure", substring(col("heure_texte"), 1, 2).cast("int"))
      .withColumn("minute", substring(col("heure_texte"), 4, 2).cast("int"))
      .withColumn("seconde", substring(col("heure_texte"), 7, 2).cast("int"))
)

df_final = (
    df_transforme
    .withColumnRenamed("production_id", "id_production")
    .withColumnRenamed("turbine_name", "nom_turbine")
    .withColumnRenamed("capacity", "capacite")
    .withColumnRenamed("location_name", "nom_emplacement")
    .withColumnRenamed("region", "region")
    .withColumnRenamed("status", "etat")
    .withColumnRenamed("responsible_department", "departement_responsable")
    .withColumnRenamed("wind_speed", "vitesse_vent")
    .withColumnRenamed("wind_direction", "direction_vent")
    .withColumnRenamed("energy_produced", "energie_produite")
)

spark.sql("CREATE SCHEMA IF NOT EXISTS wind")
df_final.write.format("delta").mode("overwrite").saveAsTable("wind.silver_production_transforme")
