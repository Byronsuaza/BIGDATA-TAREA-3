# 🚕 Análisis de Datos de NYC Taxi en Tiempo Real

## Spark Streaming + Apache Kafka

Sistema de análisis en tiempo real de datos de taxis de Nueva York utilizando Apache Spark Streaming y Apache Kafka para procesamiento de datos en streaming.

---


## 🎯 Descripción del Proyecto

Este proyecto implementa un sistema de análisis de datos en tiempo real para simular y procesar información de viajes de taxis en la ciudad de Nueva York. El sistema genera datos de viajes continuamente, los transmite a través de Apache Kafka y los procesa con Apache Spark Streaming para obtener métricas en tiempo real.

### Problema a Resolver

**Análisis de Demanda de Taxis en Tiempo Real**

El sistema permite:
- ✅ Identificar zonas con mayor demanda en tiempo real
- ✅ Calcular ingresos por zona cada minuto
- ✅ Detectar patrones de comportamiento de pasajeros
- ✅ Optimizar la distribución de taxis según demanda
- ✅ Analizar tipos de pago y propinas

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Versión | Descripción |
|------------|---------|-------------|
| **Apache Kafka** | 3.6.2 | Plataforma de streaming distribuido |
| **Apache Spark** | 3.5.3 | Motor de procesamiento de datos |
| **Python** | 3.x | Lenguaje de programación |
| **kafka-python** | Latest | Cliente de Kafka para Python |
| **PySpark** | 3.5.3 | API de Spark para Python |
| **Apache ZooKeeper** | Incluido con Kafka | Coordinación de servicios distribuidos |

---

## 📦 Requisitos Previos

### Hardware
- Mínimo 4GB RAM
- 10GB de espacio en disco

### Software
- Sistema Operativo: Linux (Ubuntu/Debian recomendado)
- Java JDK 8 o superior
- Python 3.7 o superior
- Apache Hadoop (configurado previamente)
- Apache Spark (configurado previamente)

### Credenciales de Acceso a la VM
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

### 2. Instalar Dependencias de Python

```bash
# Opción 1: Usando apt (Recomendado)
sudo apt update
sudo apt install python3-kafka

# Opción 2: Usando pip
pip install kafka-python --break-system-packages
```

### 3. Descargar e Instalar Apache Kafka

```bash
# Descargar Kafka
cd ~
wget https://archive.apache.org/dist/kafka/3.6.2/kafka_2.13-3.6.2.tgz

# Descomprimir
tar -xzf kafka_2.13-3.6.2.tgz

# Mover a directorio /opt
sudo mv kafka_2.13-3.6.2 /opt/Kafka

# Verificar instalación
ls /opt/Kafka
```

Deberías ver carpetas: `bin`, `config`, `libs`, `licenses`

---

## ⚙️ Configuración

### 1. Iniciar ZooKeeper

ZooKeeper es necesario para la coordinación de Kafka.

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
```

⏰ **Espera 5-10 segundos** hasta ver el mensaje:
```
INFO binding to port 0.0.0.0/0.0.0.0:2181
```

Presiona **Enter** para recuperar el prompt.

### 2. Iniciar Kafka Server

```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

⏰ **Espera 5-10 segundos** hasta ver:
```
INFO [KafkaServer id=0] started
```

Presiona **Enter**.

### 3. Crear el Topic de Kafka

```bash
/opt/Kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic taxi_trips
```

✅ **Respuesta esperada:** `Created topic taxi_trips`

### 4. Verificar el Topic

```bash
/opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Deberías ver: `taxi_trips`

---

## ▶️ Ejecución

### Paso 1: Ejecutar el Productor (Producer)

El productor genera datos simulados de viajes de taxi y los envía a Kafka.

**Terminal 1:**
```bash
cd BIGDATA-TAREA-3
python3 kafka_producer_taxi.py
```

**Salida esperada:**
```
🚕 NYC Taxi Data Producer - Iniciado
Enviando datos a Kafka topic: taxi_trips
--------------------------------------------------
✅ Enviado: Trip 456789 | Manhattan → Brooklyn | $23.50 | 3.2 miles
✅ Enviado: Trip 234567 | Queens → Manhattan | $31.00 | 8.5 miles
✅ Enviado: Trip 789012 | Brooklyn → Bronx | $18.75 | 6.8 miles
...
```

### Paso 2: Ejecutar el Consumidor (Consumer)

El consumidor procesa los datos con Spark Streaming y genera análisis en tiempo real.

**Terminal 2 (Nueva conexión SSH):**
```bash
cd BIGDATA-TAREA-3
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_taxi.py
```

**Salida esperada:**
```
🚕 NYC Taxi Streaming Analysis - Iniciado
============================================================

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
|window                                    |pickup_zone  |total_trips|avg_fare  |avg_distance |total_revenue |avg_passengers |
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
|{2025-10-27 10:15:00, 2025-10-27 10:16:00}|Manhattan    |45         |24.35     |2.8          |1195.75       |1.8            |
|{2025-10-27 10:15:00, 2025-10-27 10:16:00}|Brooklyn     |32         |18.50     |5.2          |624.00        |2.1            |
|{2025-10-27 10:15:00, 2025-10-27 10:16:00}|Queens       |28         |22.10     |7.5          |658.80        |1.9            |
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
```

### Paso 3: Monitorear con Spark UI

Accede a la interfaz web de Spark para ver métricas detalladas:

```
http://[TU-IP]:4040
```

Ejemplo: `http://192.168.1.7:4040`

### Paso 4: Detener la Ejecución

En ambas terminales:
```
Ctrl + C
```

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│  kafka_producer │────────▶│ Apache Kafka │────────▶│ spark_consumer  │
│   _taxi.py      │         │ Topic:       │         │    _taxi.py     │
│                 │         │ taxi_trips   │         │                 │
│ Genera datos    │         │              │         │ Procesa datos   │
│ simulados de    │         │ Almacena en  │         │ en ventanas de  │
│ viajes de taxi  │         │ buffer       │         │ tiempo          │
└─────────────────┘         └──────────────┘         └─────────────────┘
                                                                │
                                                                ▼
                                                      ┌─────────────────┐
                                                      │  Análisis en    │
                                                      │  Tiempo Real    │
                                                      │                 │
                                                      │ • Demanda/zona  │
                                                      │ • Tarifas avg   │
                                                      │ • Ingresos      │
                                                      └─────────────────┘
```

### Flujo de Datos

1. **Productor (Producer):** Genera datos simulados cada 1 segundo
2. **Kafka:** Almacena los mensajes en el topic `taxi_trips`
3. **Spark Streaming:** Lee datos de Kafka en micro-batches
4. **Procesamiento:** Agrupa datos en ventanas de 1 minuto
5. **Salida:** Muestra resultados en consola y Spark UI

---

## 📊 Análisis Realizados

### 1. Estadísticas por Zona de Recogida

Métricas calculadas cada minuto:

| Métrica | Descripción |
|---------|-------------|
| `total_trips` | Número total de viajes por zona |
| `avg_fare` | Tarifa promedio en dólares |
| `avg_distance` | Distancia promedio en millas |
| `total_revenue` | Ingresos totales generados |
| `avg_passengers` | Número promedio de pasajeros |

### 2. Ventanas de Tiempo

- **Tamaño de ventana:** 1 minuto
- **Modo de salida:** Complete (muestra todos los resultados acumulados)
- **Actualización:** Cada micro-batch

### 3. Zonas Analizadas

- Manhattan
- Brooklyn
- Queens
- Bronx
- Staten Island

---

## 📈 Resultados Esperados

### Ejemplo de Salida - Estadísticas por Zona

```
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
|window                                    |pickup_zone  |total_trips|avg_fare  |avg_distance |total_revenue |avg_passengers |
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Manhattan    |60         |26.80     |3.2          |1738.50       |1.7            |
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Brooklyn     |45         |19.35     |5.8          |945.20        |2.2            |
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Queens       |38         |23.45     |8.1          |965.40        |2.0            |
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Bronx        |25         |17.80     |6.5          |489.50        |1.9            |
|{2025-10-27 14:30:00, 2025-10-27 14:31:00}|Staten Island|12         |28.90     |10.2         |381.70        |1.5            |
+------------------------------------------+-------------+-----------+----------+-------------+--------------+---------------+
```

### Interpretación

- **Manhattan:** Mayor demanda (60 viajes) pero distancias cortas (3.2 millas)
- **Brooklyn:** Segunda mayor demanda con distancias intermedias
- **Staten Island:** Menor demanda pero tarifas más altas por distancias largas


## 📁 Estructura del Proyecto

```
BIGDATA-TAREA-3/
│
├── kafka_producer_taxi.py           # Productor de datos
├── spark_streaming_consumer_taxi.py # Consumidor con Spark Streaming
├── README.md                         # Documentación (este archivo)
└── .gitignore                        # Archivos ignorados por Git
```

---

## 📚 Dataset de Referencia

Aunque este proyecto utiliza **datos simulados**, está basado en el dataset real:

- **Nombre:** NYC Yellow Taxi Trip Data
- **Fuente:** NYC Taxi & Limousine Commission (TLC)
- **Disponible en:**
  - Kaggle: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
  - NYC Open Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Volumen:** 3+ mil millones de registros históricos

---


---

## 👨‍💻 Autor

**Byron Suaza**

GitHub: [@Byronsuaza](https://github.com/Byronsuaza)

---

## 📝 Licencia

Este proyecto es de uso académico para el curso de Big Data de la UNAD.

