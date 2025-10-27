import csv
import random
from datetime import datetime, timedelta

def generate_taxi_dataset(num_rows=50000, filename='nyc_taxi_data.csv'):
    """
    Genera un dataset simulado de viajes de taxi en formato CSV
    Basado en la estructura real del NYC Taxi dataset
    """
    
    zones = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
    payment_types = ['Credit card', 'Cash', 'No charge', 'Dispute']
    
    print(f"🚕 Generando dataset con {num_rows} registros...")
    
    # Fecha base: Enero 2024
    base_date = datetime(2024, 1, 1)
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = [
            'trip_id',
            'pickup_datetime',
            'dropoff_datetime',
            'pickup_zone',
            'dropoff_zone',
            'passenger_count',
            'trip_distance',
            'fare_amount',
            'tip_amount',
            'tolls_amount',
            'total_amount',
            'payment_type'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(num_rows):
            # Generar fecha/hora aleatoria en enero 2024
            random_seconds = random.randint(0, 30*24*60*60)  # 30 días
            pickup_time = base_date + timedelta(seconds=random_seconds)
            
            # Duración del viaje (5 min - 2 horas)
            trip_duration = random.randint(5*60, 120*60)
            dropoff_time = pickup_time + timedelta(seconds=trip_duration)
            
            # Generar zona de origen
            pickup_zone = random.choice(zones)
            dropoff_zone = random.choice(zones)
            
            # Manhattan tiene viajes más cortos y caros
            if pickup_zone == 'Manhattan':
                trip_distance = round(random.uniform(0.5, 5.0), 2)
                fare_amount = round(random.uniform(8.0, 35.0), 2)
            else:
                trip_distance = round(random.uniform(1.0, 15.0), 2)
                fare_amount = round(random.uniform(6.0, 55.0), 2)
            
            # Generar otros campos
            passenger_count = random.randint(1, 6)
            payment_type = random.choice(payment_types)
            
            # Calcular propina (solo para tarjeta de crédito)
            if payment_type == 'Credit card':
                tip_amount = round(fare_amount * random.uniform(0.10, 0.25), 2)
            else:
                tip_amount = 0.0
            
            # Peajes (10% de probabilidad)
            tolls_amount = round(random.uniform(2.0, 8.0), 2) if random.random() < 0.1 else 0.0
            
            # Total
            total_amount = round(fare_amount + tip_amount + tolls_amount + random.uniform(0.5, 2.5), 2)
            
            # Agregar algunos valores nulos/anómalos (5% de probabilidad)
            if random.random() < 0.05:
                # Introducir datos problemáticos para limpieza
                if random.random() < 0.3:
                    passenger_count = 0  # Anómalo
                elif random.random() < 0.3:
                    trip_distance = 0.0  # Anómalo
                elif random.random() < 0.3:
                    fare_amount = -5.0  # Inválido
            
            writer.writerow({
                'trip_id': 100000 + i,
                'pickup_datetime': pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
                'dropoff_datetime': dropoff_time.strftime('%Y-%m-%d %H:%M:%S'),
                'pickup_zone': pickup_zone,
                'dropoff_zone': dropoff_zone,
                'passenger_count': passenger_count,
                'trip_distance': trip_distance,
                'fare_amount': fare_amount,
                'tip_amount': tip_amount,
                'tolls_amount': tolls_amount,
                'total_amount': total_amount,
                'payment_type': payment_type
            })
            
            # Mostrar progreso
            if (i + 1) % 10000 == 0:
                print(f"   Generados {i + 1}/{num_rows} registros...")
    
    print(f"\n✅ Dataset generado exitosamente: {filename}")
    print(f"📊 Total de registros: {num_rows}")
    print(f"📁 Tamaño aproximado: {num_rows * 150 / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    # Generar 50,000 registros (puedes cambiar el número)
    generate_taxi_dataset(50000, 'nyc_taxi_data.csv')
