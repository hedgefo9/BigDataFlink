import os
import json
import time
import csv
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "mock_data_topic"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def process_csv_and_send(producer, csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(TOPIC, row)
            time.sleep(0.01)


def main():
    producer = create_producer()

    data_dir = "/app/mock_data"
    csv_files = sorted([
        os.path.join(data_dir, fname)
        for fname in os.listdir(data_dir)
        if fname.endswith(".csv")
    ])

    if not csv_files:
        print("Не найдены CSV-файлы в папке mock_data.")
        return

    print(f"Найдено {len(csv_files)} файлов. Начинаем отправку данных в Kafka…")
    for csv_file in csv_files:
        print(f"Отправка данных из {csv_file}...")
        process_csv_and_send(producer, csv_file)

    producer.flush()
    print("Все данные отправлены в Kafka.")


if __name__ == "__main__":
    main()
