from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, count, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
import logging

# Configurar el nivel de log a WARN para reducir mensajes INFO
logging.getLogger("py4j").setLevel(logging.ERROR)

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("NYCTaxiStreamingAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚕 NYC Taxi Streaming Analysis - Iniciado")
print("=" * 60)

# Definir el esquema de los datos de taxi
schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("pickup_zone", StringType()),
    StructField("dropoff_zone", StringType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", FloatType()),
    StructField("fare_amount", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("payment_type", StringType()),
    StructField("timestamp", LongType())
])

# Leer datos desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi_trips") \
    .load()

# Parsear los datos JSON y convertir timestamp
from pyspark.sql.functions import from_unixtime

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convertir timestamp Unix a timestamp de Spark
parsed_df = parsed_df.withColumn(
    "pickup_datetime", 
    from_unixtime(col("timestamp")).cast("timestamp")
)

# ============================================
# ANÁLISIS 1: Estadísticas por Zona (cada minuto)
# ============================================
zone_stats = parsed_df \
    .groupBy(
        window(col("pickup_datetime"), "1 minute"),
        "pickup_zone"
    ) \
    .agg(
        count("trip_id").alias("total_trips"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_distance"),
        sum("total_amount").alias("total_revenue"),
        avg("passenger_count").alias("avg_passengers")
    )

# ============================================
# ANÁLISIS 2: Top Zonas por Demanda
# ============================================
demand_by_zone = parsed_df \
    .groupBy(
        window(col("pickup_datetime"), "1 minute"),
        "pickup_zone"
    ) \
    .agg(
        count("trip_id").alias("demand_count")
    ) \
    .orderBy(col("demand_count").desc())

# ============================================
# ANÁLISIS 3: Análisis de Pagos
# ============================================
payment_analysis = parsed_df \
    .groupBy(
        window(col("pickup_datetime"), "1 minute"),
        "payment_type"
    ) \
    .agg(
        count("trip_id").alias("transaction_count"),
        avg("tip_amount").alias("avg_tip"),
        sum("total_amount").alias("total_amount")
    )

# ============================================
# ANÁLISIS 4: Detección de Viajes Anómalos
# ============================================
anomalies = parsed_df \
    .filter(
        (col("fare_amount") > 100) | 
        (col("trip_distance") > 20) |
        (col("passenger_count") > 5)
    ) \
    .select(
        "pickup_datetime",
        "trip_id",
        "pickup_zone",
        "dropoff_zone",
        "fare_amount",
        "trip_distance",
        "passenger_count"
    )

# ============================================
# Escribir resultados en consola
# ============================================

# Query 1: Estadísticas por zona
query1 = zone_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Zone Statistics") \
    .start()

print("\n📊 Análisis activo: Estadísticas por Zona")
print("   - Total de viajes por zona")
print("   - Tarifa promedio")
print("   - Distancia promedio")
print("   - Ingresos totales")
print("-" * 60)

# Esperar a que termine (Ctrl+C para detener)
query1.awaitTermination()
