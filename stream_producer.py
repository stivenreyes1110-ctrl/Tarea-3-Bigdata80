
import os, time, csv, json
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC           = os.getenv("KAFKA_TOPIC", "nyc_taxi_trips")
DATA_FILE       = os.getenv("DATA_FILE", "data/raw/tripdata_2019-01.csv")
RATE_MS         = int(os.getenv("RATE_MS", "1"))  # milisegundos entre mensajes

def try_float(x):
    try:
        return float(x) if x not in (None, "", " ", "NaN") else None
    except Exception:
        return None

def try_int(x):
    try:
        return int(float(x)) if x not in (None, "", " ", "NaN") else None
    except Exception:
        return None

def main():
    print(f"→ Producer conectado a {KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print(f"→ Archivo: {DATA_FILE}  | RATE_MS={RATE_MS}")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=0,
        acks="1",
    )

    with open(DATA_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            msg = {
                "tpep_pickup_datetime":  row.get("tpep_pickup_datetime"),
                "tpep_dropoff_datetime": row.get("tpep_dropoff_datetime"),
                "PULocationID":          row.get("PULocationID") or row.get("pulocationid"),
                "DOLocationID":          row.get("DOLocationID") or row.get("dolocationid"),
                "trip_distance":         try_float(row.get("trip_distance")),
                "fare_amount":           try_float(row.get("fare_amount")),
                "tip_amount":            try_float(row.get("tip_amount")),
                "total_amount":          try_float(row.get("total_amount")),
                "passenger_count":       try_int(row.get("passenger_count")),
            }
            producer.send(TOPIC, msg)
            if i % 500 == 0:
                print(f"… enviados {i:,} mensajes")
            time.sleep(RATE_MS / 1000.0)

    producer.flush()
    producer.close()
    print("✓ Producer finalizado")

if __name__ == "__main__":
    main()
PY
chmod +x streaming/stream_producer.py
