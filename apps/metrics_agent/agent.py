"""
AGENT DE METRIQUES SYSTEME (psutil) - Pure Streaming Kafka
----------------------------------------------------------
Role : Recolte l'etat de sante materiel de la machine toutes les 10 secondes.
Format : Envoie directement chaque mesure vers le topic Kafka 'aiops-metrics'.
"""

import os
import time
import psutil
from kafka_producer import send_kafka_message

# Intervalle de collecte en secondes
COLLECT_INTERVAL = 10

# Nom du topic Kafka configure via variable d'environnement
TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "aiops-metrics")


def get_system_metrics(prev_disk, prev_net, elapsed_time):
    """
    Calcule les metriques systeme actuelles et les debits Disque/Reseau par seconde.
    """
    cpu_pct = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory()
    disk_usage = psutil.disk_usage("/")

    curr_disk = psutil.disk_io_counters()
    disk_read_sec = (
        int((curr_disk.read_bytes - prev_disk.read_bytes) / elapsed_time)
        if prev_disk and elapsed_time > 0
        else 0
    )
    disk_write_sec = (
        int((curr_disk.write_bytes - prev_disk.write_bytes) / elapsed_time)
        if prev_disk and elapsed_time > 0
        else 0
    )

    curr_net = psutil.net_io_counters()
    net_recv_sec = (
        int((curr_net.bytes_recv - prev_net.bytes_recv) / elapsed_time)
        if prev_net and elapsed_time > 0
        else 0
    )
    net_sent_sec = (
        int((curr_net.bytes_sent - prev_net.bytes_sent) / elapsed_time)
        if prev_net and elapsed_time > 0
        else 0
    )

    metrics_data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "cpu_percent": float(cpu_pct),
        "ram_percent": float(ram.percent),
        "ram_available_mb": round(ram.available / (1024 * 1024), 2),
        "disk_percent": float(disk_usage.percent),
        "disk_read_bytes_sec": disk_read_sec,
        "disk_write_bytes_sec": disk_write_sec,
        "net_bytes_recv_sec": net_recv_sec,
        "net_bytes_sent_sec": net_sent_sec,
    }

    return metrics_data, curr_disk, curr_net


def main():
    print(f"Agent de metriques psutil demarre (Export streaming vers topic '{TOPIC_METRICS}')...")

    prev_disk = psutil.disk_io_counters()
    prev_net = psutil.net_io_counters()
    last_time = time.time()

    while True:
        try:
            time.sleep(COLLECT_INTERVAL)
            now = time.time()
            elapsed = now - last_time

            metrics, prev_disk, prev_net = get_system_metrics(prev_disk, prev_net, elapsed)
            last_time = now

            # Envoi direct vers Kafka (Topic: aiops-metrics)
            send_kafka_message(TOPIC_METRICS, metrics)

            print(
                f"[{metrics['timestamp']}] "
                f"CPU: {metrics['cpu_percent']}% | "
                f"RAM: {metrics['ram_percent']}% -> Envoye a Kafka"
            )

        except Exception as e:
            print(f"Erreur lors de la collecte des metriques : {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()