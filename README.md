# 🚕 Análisis de Datos de NYC Taxi en Tiempo Real

## Spark Streaming + Apache Kafka + Batch Processing

Sistema completo de análisis de datos de taxis de Nueva York utilizando Apache Spark para procesamiento batch y Apache Spark Streaming con Apache Kafka para análisis en tiempo real.


## 🎯 Descripción del Proyecto

Este proyecto implementa un sistema completo de análisis de datos con **dos componentes principales**:

1. **Procesamiento Batch**: Análisis exploratorio y limpieza de datos históricos
2. **Procesamiento Streaming**: Análisis en tiempo real de datos continuos

Ambos componentes procesan información de viajes de taxis en la ciudad de Nueva York para obtener insights sobre demanda, tarifas, rutas y patrones de comportamiento.

### Problema a Resolver

**"Sistema Integral de Análisis de Demanda de Taxis"**

El sistema permite:
- ✅ Análisis histórico de grandes volúmenes de datos (Batch)
- ✅ Limpieza y transformación de datos
- ✅ Análisis exploratorio completo (EDA)
- ✅ Monitoreo en tiempo real de demanda por zonas
- ✅ Cálculo de métricas en ventanas de tiempo
- ✅ Detección de patrones y anomalías

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Versión | Uso |
|------------|---------|-----|
| **Apache Spark** | 3.5.3 | Procesamiento batch y streaming |
| **Apache Kafka** | 3.6.2 | Plataforma de streaming distribuido |
| **Apache ZooKeeper** | Incluido | Coordinación de servicios |
| **Python** | 3.x | Lenguaje de programación |
| **PySpark** | 3.5.3 | API de Spark para Python |
| **kafka-python** | Latest | Cliente de Kafka |

---

## 📦 Requisitos Previos

### Hardware
- Mínimo 4GB RAM
- 10GB de espacio en disco

### Software
- Sistema Operativo: Linux (Ubuntu/Debian)
- Java JDK 8 o superior
- Python 3.7 o superior
- Apache Hadoop (configurado)
- Apache Spark (configurado)

### Credenciales VM
```
Usuario: vboxuser
Password: bigdata
```

---

## 🚀 Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/Byronsuaza/BIGDATA-TAREA-3.git
cd BIGDATA-TAREA-3
```

### 2. Instalar Dependencias Python

```bash
# Opción 1: Usando apt (Recomendado)
sudo apt update
sudo apt install python3-kafka

# Opción 2: Usando pip
pip install kafka-python --break-system-packages
```

### 3. Instalar Apache Kafka

```bash
# Descargar Kafka
cd ~
wget https://archive.apache.org/dist/kafka/3.6.2/kafka_2.13-3.6.2.tgz

# Descomprimir
tar -xzf kafka_2.13-3.6.2.tgz

# Mover a /opt
sudo mv kafka_2.13-3.6.2 /opt/Kafka

# Verificar
ls /opt/Kafka
```

---

## 📊 Componente 1: Procesamiento Batch

### Descripción

El componente batch realiza análisis exploratorio completo sobre datos históricos, incluyendo limpieza, transformación y generación de insights.

### 1.1 Generar Dataset

```bash
# Crear el generador
nano generate_taxi_dataset.py
# Copiar contenido del archivo generate_taxi_dataset.py del repositorio
# Guardar: Ctrl+O, Enter, Ctrl+X

# Ejecutar generador
python3 generate_taxi_dataset.py
```

**Salida esperada:**
```
🚕 Generando dataset con 50000 registros...
   Generados 10000/50000 registros...
   Generados 20000/50000 registros...
   ...
✅ Dataset generado exitosamente: nyc_taxi_data.csv
📊 Total de registros: 50000
📁 Tamaño aproximado: 7.50 MB
```

### 1.2 Ejecutar Procesamiento Batch

```bash
# Crear el script
nano batch_processing_taxi.py
# Copiar contenido del archivo batch_processing_taxi.py del repositorio
# Guardar: Ctrl+O, Enter, Ctrl+X

# Ejecutar con Spark
spark-submit batch_processing_taxi.py
```

### 1.3 Operaciones Realizadas

#### ✅ Carga de Datos
- Lee 50,000 registros desde CSV
- Infiere esquema automáticamente
- Muestra estructura de datos

#### ✅ Análisis Exploratorio (EDA)
- Estadísticas descriptivas completas
- Distribuciones por zona geográfica
- Análisis por tipo de pago
- Identificación de rutas populares
- Métricas de ingresos

#### ✅ Limpieza de Datos
- Eliminación de valores nulos
- Filtrado de valores anómalos:
  - Pasajeros = 0
  - Distancia = 0
  - Tarifas negativas
  - Outliers extremos (> $500)
- Eliminación de duplicados

#### ✅ Transformación
Columnas derivadas creadas:
- `trip_duration_minutes`: Duración del viaje
- `fare_per_mile`: Tarifa por milla
- `tip_percentage`: Porcentaje de propina

#### ✅ Almacenamiento
Resultados guardados en `output_batch_processing/`:
- **clean_data/**: Datos limpios (Parquet)
- **zone_analysis/**: Análisis por zona (CSV)
- **payment_analysis/**: Análisis de pagos (CSV)
- **popular_routes/**: Top 100 rutas (CSV)

### 1.4 Ejemplo de Resultados Batch

```
🗺️  Análisis por Zona de Recogida:
+-------------+-----------+--------+------------+-------+-------------+
|pickup_zone  |total_trips|avg_fare|avg_distance|avg_tip|total_revenue|
+-------------+-----------+--------+------------+-------+-------------+
|Manhattan    |15234      |24.56   |3.45        |4.23   |423567.89    |
|Brooklyn     |12456      |19.34   |5.67        |2.89   |278945.12    |
|Queens       |10987      |21.45   |6.78        |3.12   |256789.45    |
|Bronx        |5678       |18.90   |7.23        |2.45   |123456.78    |
|Staten Island|3195       |27.89   |10.45       |3.67   |98765.43     |
+-------------+-----------+--------+------------+-------+-------------+
```

---

## ⚡ Componente 2: Procesamiento en Tiempo Real

### Descripción

Sistema de streaming que procesa datos de taxis en tiempo real utilizando Kafka como broker de mensajes y Spark Streaming para análisis continuo.

### 2.1 Configurar Kafka

#### Iniciar ZooKeeper
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
```
⏰ Espera 5-10 segundos y presiona **Enter**

#### Iniciar Kafka Server
```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```
⏰ Espera 5-10 segundos y presiona **Enter**

#### Crear Topic
```bash
/opt/Kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic taxi_trips
```

✅ Deberías ver: `Created topic taxi_trips`

### 2.2 Ejecutar Producer (Terminal 1)

```bash
# Crear el productor
nano kafka_producer_taxi.py
# Copiar contenido del repositorio
# Guardar: Ctrl+O, Enter, Ctrl+X

# Ejecutar
python3 kafka_producer_taxi.py
```

**Salida esperada:**
```
🚕 NYC Taxi Data Producer - Iniciado
Enviando datos a Kafka topic: taxi_trips
--------------------------------------------------
✅ Enviado: Trip 456789 | Manhattan → Brooklyn | $23.50 | 3.2 miles
✅ Enviado: Trip 234567 | Queens → Manhattan | $31.00 | 8.5 miles
```

### 2.3 Ejecutar Consumer (Terminal 2 - Nueva conexión SSH)

```bash
# Crear el consumidor
nano spark_streaming_consumer_taxi.py
# Copiar contenido del repositorio
# Guardar: Ctrl+O, Enter, Ctrl+X

# Ejecutar con Spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_taxi.py
```

**Salida esperada:**
```
🚕 NYC Taxi Streaming Analysis - Iniciado
============================================================

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+-------------+-----------+--------+
|window                                    |pickup_zone  |total_trips|avg_fare|
+------------------------------------------+-------------+-----------+--------+
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Manhattan    |60         |26.80   |
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Brooklyn     |45         |19.35   |
+------------------------------------------+-------------+-----------+--------+
```

### 2.4 Monitorear con Spark UI

```
http://[TU-IP]:4040
```

Ejemplo: `http://192.168.1.7:4040`

### 2.5 Detener Ejecución

En ambas terminales: **Ctrl + C**

---

## 🏗️ Arquitectura del Sistema

### Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                     PROCESAMIENTO BATCH                         │
│                                                                 │
│  CSV Dataset  →  Spark Batch  →  Limpieza/EDA  →  Resultados   │
│  (50K registros)    (PySpark)     (Análisis)      (Parquet/CSV)│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  PROCESAMIENTO EN TIEMPO REAL                   │
│                                                                 │
│  Producer  →  Kafka Topic  →  Spark Streaming  →  Dashboard    │
│  (Python)     (taxi_trips)     (PySpark)          (Console/UI) │
│                                                                 │
│  Genera datos  │  Buffer     │  Ventanas de    │  Métricas en  │
│  simulados     │  streaming  │  1 minuto       │  tiempo real  │
└─────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos - Streaming

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│  kafka_producer │────────▶│ Apache Kafka │────────▶│ spark_consumer  │
│   _taxi.py      │         │ Topic:       │         │    _taxi.py     │
│                 │         │ taxi_trips   │         │                 │
│ Genera 1 viaje  │         │              │         │ Ventanas de     │
│ por segundo     │         │ Queue/Buffer │         │ tiempo (1 min)  │
└─────────────────┘         └──────────────┘         └─────────────────┘
                                                                │
                                                                ▼
                                                      ┌─────────────────┐
                                                      │  Análisis       │
                                                      │  • Trips/zona   │
                                                      │  • Tarifas avg  │
                                                      │  • Ingresos     │
                                                      └─────────────────┘
```

---

## 📊 Resultados y Análisis

### Métricas del Sistema Batch

| Métrica | Valor |
|---------|-------|
| Registros procesados | 50,000 |
| Registros limpios | ~47,500 |
| Registros eliminados | ~2,500 |
| Tiempo de procesamiento | ~45 segundos |
| Archivos generados | 4 |

### Métricas del Sistema Streaming

| Métrica | Descripción |
|---------|-------------|
| Frecuencia de datos | 1 registro/segundo |
| Ventana de tiempo | 1 minuto |
| Zonas monitoreadas | 5 |
| Métricas por ventana | 5 |
| Latencia | < 2 segundos |

### Insights Generados

#### Batch Processing
- ✅ Distribución de viajes por zona
- ✅ Análisis de tarifas y propinas
- ✅ Identificación de rutas populares
- ✅ Detección de valores anómalos
- ✅ Patrones de pago

#### Streaming
- ✅ Demanda en tiempo real por zona
- ✅ Ingresos por minuto
- ✅ Ocupación promedio
- ✅ Distancias promedio
- ✅ Alertas de picos de demanda

---

## 📁 Estructura del Proyecto

```
BIGDATA-TAREA-3/
│
├── generate_taxi_dataset.py              # Generador de datos
├── batch_processing_taxi.py              # Procesamiento batch
├── kafka_producer_taxi.py                # Producer de streaming
├── spark_streaming_consumer_taxi.py      # Consumer de streaming
├── README.md                             # Este archivo
│
├── nyc_taxi_data.csv                     # Dataset generado
│
└── output_batch_processing/              # Resultados batch
    ├── clean_data/                       # Datos limpios (Parquet)
    ├── zone_analysis/                    # Análisis por zona (CSV)
    ├── payment_analysis/                 # Análisis de pagos (CSV)
    └── popular_routes/                   # Rutas populares (CSV)
```

---

## 📚 Dataset de Referencia

Este proyecto utiliza **datos simulados** basados en el dataset real:

- **Nombre:** NYC Yellow Taxi Trip Data
- **Fuente:** NYC Taxi & Limousine Commission (TLC)
- **Disponible en:**
  - Kaggle: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
  - NYC Open Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Volumen real:** 3+ mil millones de registros históricos
- **Simulado:** 50,000 registros generados

---


## 👨‍💻 Autor

**Byron Suaza**
