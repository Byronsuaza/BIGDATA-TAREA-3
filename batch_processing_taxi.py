from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum, min, max, stddev, 
    when, isnan, isnull, hour, dayofweek, 
    datediff, to_timestamp, round as spark_round
)
from pyspark.sql.types import DoubleType
import time

print("=" * 80)
print("🚕 NYC TAXI - BATCH PROCESSING & EDA")
print("=" * 80)

# ============================================
# 1. CREAR SESIÓN DE SPARK
# ============================================
print("\n📊 Iniciando Spark Session...")
spark = SparkSession.builder \
    .appName("NYC_Taxi_Batch_Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session iniciado correctamente\n")

# ============================================
# 2. CARGAR DATOS
# ============================================
print("📥 PASO 1: CARGA DE DATOS")
print("-" * 80)

# Cambiar la ruta según tu archivo
# Opción A: CSV generado
csv_path = "nyc_taxi_data.csv"

print(f"Leyendo archivo: {csv_path}")
start_time = time.time()

# Leer CSV
df = spark.read.csv(csv_path, header=True, inferSchema=True)


load_time = time.time() - start_time
print(f"✅ Datos cargados en {load_time:.2f} segundos")
print(f"📊 Total de registros: {df.count()}")
print(f"📋 Total de columnas: {len(df.columns)}")

# ============================================
# 3. EXPLORACIÓN INICIAL
# ============================================
print("\n📊 PASO 2: ANÁLISIS EXPLORATORIO DE DATOS (EDA)")
print("-" * 80)

print("\n🔍 Esquema del DataFrame:")
df.printSchema()

print("\n📋 Primeros 10 registros:")
df.show(10, truncate=False)

print("\n📊 Estadísticas Descriptivas:")
df.describe().show()

# ============================================
# 4. CALIDAD DE DATOS
# ============================================
print("\n🧹 PASO 3: ANÁLISIS DE CALIDAD DE DATOS")
print("-" * 80)

# Contar valores nulos por columna
print("\n❌ Valores nulos por columna:")
total_records = df.count()
for column in df.columns:
    # Solo usar isnan() para columnas numéricas
    col_type = df.schema[column].dataType.typeName()
    if col_type in ['double', 'float']:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
    else:
        null_count = df.filter(col(column).isNull()).count()
    
    if null_count > 0:
        percentage = (null_count / total_records) * 100
        print(f"   {column}: {null_count} ({percentage:.2f}%)")
      
# Detectar valores anómalos
print("\n⚠️  Valores Anómalos Detectados:")

# Pasajeros = 0
zero_passengers = df.filter(col("passenger_count") == 0).count()
print(f"   Viajes con 0 pasajeros: {zero_passengers}")

# Distancia = 0
zero_distance = df.filter(col("trip_distance") == 0).count()
print(f"   Viajes con distancia 0: {zero_distance}")

# Tarifas negativas
negative_fare = df.filter(col("fare_amount") < 0).count()
print(f"   Tarifas negativas: {negative_fare}")

# Tarifas muy altas (posibles outliers)
high_fare = df.filter(col("fare_amount") > 200).count()
print(f"   Tarifas > $200: {high_fare}")

# ============================================
# 5. LIMPIEZA DE DATOS
# ============================================
print("\n🧹 PASO 4: LIMPIEZA Y TRANSFORMACIÓN DE DATOS")
print("-" * 80)

print("\n🔧 Aplicando reglas de limpieza...")

# Crear DataFrame limpio
df_clean = df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("fare_amount") < 500) &  # Eliminar outliers extremos
    (col("total_amount") > 0)
)

# Eliminar duplicados
df_clean = df_clean.dropDuplicates()

records_removed = df.count() - df_clean.count()
print(f"✅ Registros eliminados: {records_removed}")
print(f"✅ Registros limpios: {df_clean.count()}")

# Crear nuevas columnas derivadas
print("\n🔧 Creando columnas derivadas...")

df_clean = df_clean.withColumn(
    "trip_duration_minutes",
    (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
)

df_clean = df_clean.withColumn(
    "fare_per_mile",
    spark_round(col("fare_amount") / col("trip_distance"), 2)
)

df_clean = df_clean.withColumn(
    "tip_percentage",
    spark_round((col("tip_amount") / col("fare_amount")) * 100, 2)
)

print("✅ Columnas derivadas creadas")

# ============================================
# 6. ANÁLISIS EXPLORATORIO AVANZADO
# ============================================
print("\n📊 PASO 5: ANÁLISIS EXPLORATORIO AVANZADO")
print("-" * 80)

# Análisis por zona de recogida
print("\n🗺️  Análisis por Zona de Recogida:")
zone_analysis = df_clean.groupBy("pickup_zone").agg(
    count("trip_id").alias("total_trips"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("trip_distance"), 2).alias("avg_distance"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip"),
    spark_round(sum("total_amount"), 2).alias("total_revenue")
).orderBy(col("total_trips").desc())

zone_analysis.show(10)

# Análisis por tipo de pago
print("\n💳 Análisis por Tipo de Pago:")
payment_analysis = df_clean.groupBy("payment_type").agg(
    count("trip_id").alias("total_trips"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("tip_amount"), 2).alias("avg_tip"),
    spark_round(sum("total_amount"), 2).alias("total_revenue")
).orderBy(col("total_trips").desc())

payment_analysis.show()

# Análisis por número de pasajeros
print("\n👥 Análisis por Número de Pasajeros:")
passenger_analysis = df_clean.groupBy("passenger_count").agg(
    count("trip_id").alias("total_trips"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare"),
    spark_round(avg("trip_distance"), 2).alias("avg_distance")
).orderBy("passenger_count")

passenger_analysis.show()

# Rutas más populares
print("\n🛣️  Top 10 Rutas Más Populares:")
routes = df_clean.groupBy("pickup_zone", "dropoff_zone").agg(
    count("trip_id").alias("total_trips"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare")
).orderBy(col("total_trips").desc())

routes.show(10)

# ============================================
# 7. ESTADÍSTICAS AVANZADAS
# ============================================
print("\n📈 PASO 6: ESTADÍSTICAS AVANZADAS")
print("-" * 80)

print("\n📊 Métricas Generales:")
general_stats = df_clean.agg(
    count("trip_id").alias("Total Trips"),
    spark_round(avg("fare_amount"), 2).alias("Avg Fare"),
    spark_round(min("fare_amount"), 2).alias("Min Fare"),
    spark_round(max("fare_amount"), 2).alias("Max Fare"),
    spark_round(stddev("fare_amount"), 2).alias("Std Dev Fare"),
    spark_round(avg("trip_distance"), 2).alias("Avg Distance"),
    spark_round(sum("total_amount"), 2).alias("Total Revenue")
)

general_stats.show(vertical=True)

# Distribución de tarifas
print("\n💰 Distribución de Tarifas:")
fare_distribution = df_clean.selectExpr(
    "count(*) as total",
    "sum(case when fare_amount < 10 then 1 else 0 end) as under_10",
    "sum(case when fare_amount >= 10 and fare_amount < 20 then 1 else 0 end) as fare_10_20",
    "sum(case when fare_amount >= 20 and fare_amount < 30 then 1 else 0 end) as fare_20_30",
    "sum(case when fare_amount >= 30 and fare_amount < 50 then 1 else 0 end) as fare_30_50",
    "sum(case when fare_amount >= 50 then 1 else 0 end) as over_50"
)

fare_distribution.show()

# ============================================
# 8. GUARDAR RESULTADOS
# ============================================
print("\n💾 PASO 7: ALMACENAMIENTO DE RESULTADOS")
print("-" * 80)

# Crear directorio para resultados
output_dir = "output_batch_processing"

print(f"\n📁 Guardando resultados en: {output_dir}/")

# Guardar DataFrame limpio
print("   1. Guardando datos limpios (Parquet)...")
df_clean.write.mode("overwrite").parquet(f"{output_dir}/clean_data")
print("   ✅ Datos limpios guardados")

# Guardar análisis por zona
print("   2. Guardando análisis por zona (CSV)...")
zone_analysis.write.mode("overwrite").csv(f"{output_dir}/zone_analysis", header=True)
print("   ✅ Análisis por zona guardado")

# Guardar análisis de pagos
print("   3. Guardando análisis de pagos (CSV)...")
payment_analysis.write.mode("overwrite").csv(f"{output_dir}/payment_analysis", header=True)
print("   ✅ Análisis de pagos guardado")

# Guardar rutas populares
print("   4. Guardando rutas populares (CSV)...")
routes.limit(100).write.mode("overwrite").csv(f"{output_dir}/popular_routes", header=True)
print("   ✅ Rutas populares guardadas")

# ============================================
# 9. RESUMEN FINAL
# ============================================
print("\n" + "=" * 80)
print("✅ PROCESAMIENTO BATCH COMPLETADO EXITOSAMENTE")
print("=" * 80)

print(f"""
📊 RESUMEN DE RESULTADOS:
   • Registros originales: {df.count()}
   • Registros limpios: {df_clean.count()}
   • Registros eliminados: {records_removed}
   • Tiempo de procesamiento: {time.time() - start_time:.2f} segundos
   
📁 ARCHIVOS GENERADOS:
   • {output_dir}/clean_data/ (Parquet)
   • {output_dir}/zone_analysis/ (CSV)
   • {output_dir}/payment_analysis/ (CSV)
   • {output_dir}/popular_routes/ (CSV)
   
🎯 SIGUIENTE PASO:
   Revisar los archivos generados en la carpeta: {output_dir}/
""")

# Detener Spark
spark.stop()
print("🛑 Spark Session detenido\n")
