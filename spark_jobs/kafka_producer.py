# spark_jobs/kafka_producer.py
import json
import time
from kafka import KafkaProducer

def main(source_path="sample_data/claims_submitted.jsonl", topic="claims.submitted", delay_seconds=0.05):
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    with open(source_path) as f:
        for i, line in enumerate(f):
            claim = json.loads(line)
            producer.send(topic, key=claim["claim_id"], value=claim)
            if i % 500 == 0:
                print(f"Produced {i} claims...")
            time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print("Done producing.")

if __name__ == "__main__":
    main()