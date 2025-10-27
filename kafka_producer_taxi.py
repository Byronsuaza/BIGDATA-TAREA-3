import time
import json
import random
from kafka import KafkaProducer
from datetime import datetime

def generate_taxi_data():
    """
    Genera datos simulados de viajes de taxi en NYC
    Basado en patrones reales del dataset
    """
    zones = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
    payment_types = ['credit_card', 'cash', 'no_charge', 'dispute']
    
    # Simular diferentes rangos según zona
    zone = random.choice(zones)
    
    # Manhattan tiene viajes más caros y cortos
    if zone == 'Manhattan':
        trip_distance = round(random.uniform(0.5, 5.0), 2)
        fare_amount = round(random.uniform(8.0, 35.0), 2)
    else:
        trip_distance = round(random.uniform(1.0, 15.0), 2)
        fare_amount = round(random.uniform(6.0, 55.0), 2)
    
    return {
        "trip_id": random.randint(100000, 999999),
        "pickup_zone": zone,
        "dropoff_zone": random.choice(zones),
        "passenger_count": random.randint(1, 6),
        "trip_distance": trip_distance,
        "fare_amount": fare_amount,
        "tip_amount": round(random.uniform(0.0, fare_amount * 0.25), 2),
        "total_amount": round(fare_amount + random.uniform(0.5, 3.0), 2),
        "payment_type": random.choice(payment_types),
        "timestamp": int(time.time())
    }

# Configurar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("🚕 NYC Taxi Data Producer - Iniciado")
print("Enviando datos a Kafka topic: taxi_trips")
print("-" * 50)

# Generar y enviar datos continuamente
try:
    while True:
        taxi_data = generate_taxi_data()
        producer.send('taxi_trips', value=taxi_data)
        print(f"✅ Enviado: Trip {taxi_data['trip_id']} | "
              f"{taxi_data['pickup_zone']} → {taxi_data['dropoff_zone']} | "
              f"${taxi_data['fare_amount']} | "
              f"{taxi_data['trip_distance']} miles")
        time.sleep(1)  # Enviar un registro por segundo
except KeyboardInterrupt:
    print("\n🛑 Producer detenido")
    producer.close()
