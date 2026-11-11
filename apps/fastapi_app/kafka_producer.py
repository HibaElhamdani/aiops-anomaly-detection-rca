"""
Helper Kafka Producer non-bloquant et ultra-léger.
"""
import os
import json
import logging
from kafka import KafkaProducer

KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

logger = logging.getLogger("AIOps_Producer")
_producer = None


def get_producer():
    global _producer
    if not KAFKA_ENABLED:
        return None

    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1,
                retries=1,
                max_block_ms=100,  # Ne bloque jamais l'application plus de 100ms
            )
            logger.info("Connecté à Kafka avec succès.")
        except Exception as e:
            logger.warning(f"Impossible de joindre Kafka: {e}")
            _producer = None

    return _producer


def send_kafka_message(topic: str, data: dict):
    if not KAFKA_ENABLED:
        return

    producer = get_producer()
    if producer is None:
        return

    try:
        producer.send(topic, value=data)
    except Exception as e:
        logger.error(f"Erreur d'envoi vers {topic}: {e}")