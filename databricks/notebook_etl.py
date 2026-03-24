# Databricks notebook source
# NYC Taxi ETL – Delta Live Tables Pipeline
# Ejecutar como: Databricks Job con parámetros:
#   input_path  = abfss://raw@<account>.dfs.core.windows.net/nyc-taxi/YYYY/MM/DD/yellow_tripdata_2024-01.parquet
#   output_container = processed
#   run_date    = YYYY-MM-DD

# COMMAND ----------
# MAGIC %md
# MAGIC ## NYC Taxi ETL — Notebook principal
# MAGIC
# MAGIC **Flujo:**
# MAGIC 1. Ingesta raw desde Blob Storage (ABFSS)
# MAGIC 2. Limpieza y validación de tipos
# MAGIC 3. Cálculo de métricas (duración, tarifa promedio, velocidad)
# MAGIC 4. Agregaciones por zona y hora
# MAGIC 5. Particionado por fecha → Delta tables
# MAGIC 6. Escritura en Azure SQL

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime

# ─── Parámetros del Job ──────────────────────────────────────────────────────
dbutils.widgets.text("input_path",       "", "Ruta ABFSS del parquet")
dbutils.widgets.text("output_container", "processed", "Container de salida")
dbutils.widgets.text("run_date",         "", "Fecha de proceso (YYYY-MM-DD)")

INPUT_PATH       = dbutils.widgets.get("input_path")
OUTPUT_CONTAINER = dbutils.widgets.get("output_container")
RUN_DATE         = dbutils.widgets.get("run_date") or datetime.utcnow().strftime("%Y-%m-%d")

STORAGE_ACCOUNT  = spark.conf.get("fs.azure.account.name", "")
DELTA_BASE       = f"abfss://{OUTPUT_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta"
SQL_JDBC_URL     = spark.conf.get("spark.sql.jdbc.url", "")
SQL_USER         = spark.conf.get("spark.sql.jdbc.user", "")
SQL_PASSWORD     = spark.conf.get("spark.sql.jdbc.password", "")

print(f"input_path       = {INPUT_PATH}")
print(f"output_container = {OUTPUT_CONTAINER}")
print(f"run_date         = {RUN_DATE}")

# COMMAND ----------
# MAGIC %md ### 1. Ingesta raw

# COMMAND ----------

def ingest_raw(path: str) -> DataFrame:
    """Lee el parquet crudo desde Blob Storage."""
    df = spark.read.parquet(path)
    print(f"[ingesta] filas brutas: {df.count():,}")
    return df

df_raw = ingest_raw(INPUT_PATH)
df_raw.printSchema()

# COMMAND ----------
# MAGIC %md ### 2. Limpieza y validación

# COMMAND ----------

# Schema esperado de NYC Yellow Taxi 2024
EXPECTED_SCHEMA = {
    "VendorID":              T.IntegerType(),
    "tpep_pickup_datetime":  T.TimestampType(),
    "tpep_dropoff_datetime": T.TimestampType(),
    "passenger_count":       T.DoubleType(),
    "trip_distance":         T.DoubleType(),
    "RatecodeID":            T.DoubleType(),
    "store_and_fwd_flag":    T.StringType(),
    "PULocationID":          T.IntegerType(),
    "DOLocationID":          T.IntegerType(),
    "payment_type":          T.LongType(),
    "fare_amount":           T.DoubleType(),
    "extra":                 T.DoubleType(),
    "mta_tax":               T.DoubleType(),
    "tip_amount":            T.DoubleType(),
    "tolls_amount":          T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "total_amount":          T.DoubleType(),
    "congestion_surcharge":  T.DoubleType(),
    "Airport_fee":           T.DoubleType(),
}


def clean_and_validate(df: DataFrame) -> DataFrame:
    """
    Limpia y normaliza el DataFrame:
    - Renombra columnas a snake_case
    - Castea tipos
    - Filtra registros inválidos
    - Elimina duplicados
    """
    # Renombrar a snake_case
    df = (df
        .withColumnRenamed("VendorID",              "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("passenger_count",       "passenger_count")
        .withColumnRenamed("trip_distance",         "trip_distance")
        .withColumnRenamed("RatecodeID",            "rate_code_id")
        .withColumnRenamed("store_and_fwd_flag",    "store_and_fwd_flag")
        .withColumnRenamed("PULocationID",          "pu_location_id")
        .withColumnRenamed("DOLocationID",          "do_location_id")
        .withColumnRenamed("payment_type",          "payment_type")
        .withColumnRenamed("fare_amount",           "fare_amount")
        .withColumnRenamed("extra",                 "extra")
        .withColumnRenamed("mta_tax",               "mta_tax")
        .withColumnRenamed("tip_amount",            "tip_amount")
        .withColumnRenamed("tolls_amount",          "tolls_amount")
        .withColumnRenamed("improvement_surcharge", "improvement_surcharge")
        .withColumnRenamed("total_amount",          "total_amount")
        .withColumnRenamed("congestion_surcharge",  "congestion_surcharge")
        .withColumnRenamed("Airport_fee",           "airport_fee")
    )

    # Castear tipos numéricos seguros
    df = df.withColumn("passenger_count",  F.col("passenger_count").cast(T.IntegerType()))
    df = df.withColumn("pu_location_id",   F.col("pu_location_id").cast(T.IntegerType()))
    df = df.withColumn("do_location_id",   F.col("do_location_id").cast(T.IntegerType()))
    df = df.withColumn("vendor_id",        F.col("vendor_id").cast(T.IntegerType()))
    df = df.withColumn("payment_type",     F.col("payment_type").cast(T.IntegerType()))

    # Filtros de calidad — reglas de negocio NYC Taxi
    df_clean = df.filter(
        (F.col("pickup_datetime").isNotNull())
        & (F.col("dropoff_datetime").isNotNull())
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        & (F.col("trip_distance") >= 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & (F.col("passenger_count").between(1, 6))
        & (F.col("pu_location_id").isNotNull())
        & (F.col("do_location_id").isNotNull())
        & (F.col("trip_distance") <= 500)           # máximo razonable en millas
        & (F.col("fare_amount") <= 1000)            # tarifa máxima razonable
    )

    # Eliminar duplicados exactos
    df_clean = df_clean.dropDuplicates()

    rows_in  = df.count()
    rows_out = df_clean.count()
    pct_kept = rows_out / rows_in * 100 if rows_in > 0 else 0
    print(f"[limpieza] {rows_in:,} → {rows_out:,} filas ({pct_kept:.1f}% conservadas)")

    return df_clean


df_clean = clean_and_validate(df_raw)

# COMMAND ----------
# MAGIC %md ### 3. Cálculo de métricas

# COMMAND ----------

def calculate_metrics(df: DataFrame) -> DataFrame:
    """
    Añade columnas calculadas:
    - trip_duration_minutes
    - speed_mph
    - fare_per_mile
    - fare_per_minute
    - tip_pct
    - is_airport_trip
    - pickup_hour / pickup_date (partición)
    """
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
    )
    df = df.withColumn(
        "speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0),
        ).otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "fare_per_mile",
        F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))
         .otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "fare_per_minute",
        F.when(F.col("trip_duration_minutes") > 0, F.col("fare_amount") / F.col("trip_duration_minutes"))
         .otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "tip_pct",
        F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount") * 100)
         .otherwise(F.lit(0.0)),
    )
    # JFK = 132, LGA = 138, EWR = 1
    airport_zones = [1, 132, 138]
    df = df.withColumn(
        "is_airport_trip",
        F.col("pu_location_id").isin(airport_zones) | F.col("do_location_id").isin(airport_zones),
    )
    df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
    df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))
    df = df.withColumn("pickup_year",  F.year("pickup_datetime"))
    df = df.withColumn("pickup_month", F.month("pickup_datetime"))
    df = df.withColumn("pickup_day",   F.dayofmonth("pickup_datetime"))

    # Filtrar métricas anómalas (velocidades > 200 mph son errores GPS)
    df = df.filter(
        F.col("speed_mph").isNull() | (F.col("speed_mph") <= 200)
    )

    print(f"[métricas] columnas calculadas añadidas: {len(df.columns)}")
    return df


df_metrics = calculate_metrics(df_clean)

# COMMAND ----------
# MAGIC %md ### 4. Agregaciones por zona y hora

# COMMAND ----------

def aggregate_zone_hour(df: DataFrame) -> DataFrame:
    """
    Agrega viajes por zona de pickup y hora del día.
    Resultado: una fila por (pickup_date, pu_location_id, pickup_hour)
    """
    agg = df.groupBy("pickup_date", "pu_location_id", "pickup_hour").agg(
        F.count("*").alias("trip_count"),
        F.sum("passenger_count").alias("total_passengers"),
        F.avg("trip_distance").alias("avg_distance_miles"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_amount").alias("avg_tip"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("speed_mph").alias("avg_speed_mph"),
        F.sum(F.col("is_airport_trip").cast(T.IntegerType())).alias("airport_trips"),
        F.countDistinct("do_location_id").alias("unique_destinations"),
    ).withColumn("report_date", F.lit(RUN_DATE))

    print(f"[agregaciones zona/hora] {agg.count():,} combinaciones")
    return agg


def aggregate_daily_metrics(df: DataFrame) -> DataFrame:
    """Métricas resumen del día completo."""
    agg = df.groupBy("pickup_date").agg(
        F.count("*").alias("total_trips"),
        F.sum("passenger_count").alias("total_passengers"),
        F.avg("trip_distance").alias("avg_distance_miles"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("speed_mph").alias("avg_speed_mph"),
        F.sum(F.col("is_airport_trip").cast(T.IntegerType())).alias("airport_trips"),
        F.approx_count_distinct("pu_location_id").alias("active_pickup_zones"),
    ).withColumn("metric_date", F.lit(RUN_DATE))

    print(f"[métricas diarias] {agg.count():,} filas")
    return agg


df_zone_hour = aggregate_zone_hour(df_metrics)
df_daily     = aggregate_daily_metrics(df_metrics)

# COMMAND ----------
# MAGIC %md ### 5. Escritura en Delta (particionado por fecha)

# COMMAND ----------

def write_delta(df: DataFrame, table_path: str, partition_cols: list, table_name: str):
    """Escribe en formato Delta, particionado, con merge (upsert)."""
    full_path = f"{DELTA_BASE}/{table_path}"

    if DeltaTable.isDeltaTable(spark, full_path):
        print(f"[delta] Merge en {table_name}...")
        delta_table = DeltaTable.forPath(spark, full_path)
        # Merge por fecha de pickup para idempotencia
        merge_condition = " AND ".join([f"existing.{c} = updates.{c}" for c in partition_cols])
        (delta_table.alias("existing")
            .merge(df.alias("updates"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        print(f"[delta] Creando tabla {table_name}...")
        (df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .option("overwriteSchema", "true")
            .save(full_path))

    # Registrar como tabla en el catálogo de Databricks
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS nyc_taxi.{table_name}
        USING DELTA LOCATION '{full_path}'
    """)
    print(f"[delta] ✓ {table_name} en {full_path}")


# Crear la base de datos si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS nyc_taxi")

write_delta(
    df=df_metrics,
    table_path="trips",
    partition_cols=["pickup_year", "pickup_month", "pickup_day"],
    table_name="trips",
)

write_delta(
    df=df_zone_hour,
    table_path="agg_zone_hour",
    partition_cols=["pickup_date", "pu_location_id"],
    table_name="agg_zone_hour",
)

write_delta(
    df=df_daily,
    table_path="metrics_daily",
    partition_cols=["pickup_date"],
    table_name="metrics_daily",
)

# COMMAND ----------
# MAGIC %md ### 6. Escritura en Azure SQL

# COMMAND ----------

def write_to_sql(df: DataFrame, table: str, mode: str = "append"):
    """Escribe un DataFrame en Azure SQL via JDBC."""
    if not SQL_JDBC_URL:
        print(f"[SQL] SKIP: SQL_JDBC_URL no configurado (tabla: {table})")
        return

    (df.write
        .format("jdbc")
        .option("url", SQL_JDBC_URL)
        .option("dbtable", f"dbo.{table}")
        .option("user", SQL_USER)
        .option("password", SQL_PASSWORD)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("batchsize", 10000)
        .option("truncate", mode == "overwrite")
        .mode(mode)
        .save())
    print(f"[SQL] ✓ {table} escrito en Azure SQL ({mode})")


# Selección de columnas relevantes para SQL (evitar columnas partición redundantes)
sql_trips_cols = [
    "pickup_datetime", "dropoff_datetime", "vendor_id",
    "passenger_count", "trip_distance", "pu_location_id", "do_location_id",
    "payment_type", "fare_amount", "tip_amount", "total_amount",
    "trip_duration_minutes", "speed_mph", "fare_per_mile", "tip_pct",
    "is_airport_trip", "pickup_hour", "pickup_date",
]

write_to_sql(df_metrics.select(sql_trips_cols),  "nyc_taxi_trips",          mode="append")
write_to_sql(df_zone_hour,                        "nyc_taxi_agg_zone_hour",  mode="append")
write_to_sql(df_daily,                            "nyc_taxi_metrics_daily",  mode="append")

# COMMAND ----------
# MAGIC %md ### 7. Estadísticas finales del run

# COMMAND ----------

print("\n" + "="*60)
print(f"NYC TAXI ETL — RESUMEN DEL RUN")
print("="*60)
print(f"Fecha procesada  : {RUN_DATE}")
print(f"Filas brutas     : {df_raw.count():,}")
print(f"Filas limpias    : {df_clean.count():,}")
print(f"Filas con métric : {df_metrics.count():,}")
print(f"Agg zona/hora    : {df_zone_hour.count():,}")
print(f"Métricas diarias : {df_daily.count():,}")
print("="*60)

dbutils.notebook.exit("SUCCESS")