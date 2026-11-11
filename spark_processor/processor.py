import os
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, stddev, max as spark_max,
    when, abs as spark_abs, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType
)

# =============================================================================
# 1. SESSION SPARK
# =============================================================================
spark = SparkSession.builder \
    .appName("AIOps-Medallion-Processor") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# =============================================================================
# 2. SCHEMAS
# =============================================================================
metrics_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("cpu_percent", FloatType(), True),
    StructField("ram_percent", FloatType(), True),
    StructField("disk_usage_percent", FloatType(), True),
    StructField("net_sent_mb", FloatType(), True),
    StructField("net_recv_mb", FloatType(), True),
    StructField("disk_read_mb", FloatType(), True),
    StructField("disk_write_mb", FloatType(), True),
    StructField("process_count", IntegerType(), True)
])

logs_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("method", StringType(), True),
    StructField("path", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("error_message", StringType(), True)
])

# =============================================================================
# 3. REGLES D'ANOMALIE PAR METRIQUE
#    nom court -> colonne source, seuil critique (moyenne), seuil "valeur haute" (max)
#    Si critical_avg / high_max = None : pas de seuil absolu, on exige un pic
#    d'au moins 2x la moyenne en plus du Z-score.
# =============================================================================
Z_THRESHOLD = 3.0

METRICS = {
    "cpu":        {"src": "cpu_percent",        "critical_avg": 85.0, "high_max": 70.0},
    "ram":        {"src": "ram_percent",        "critical_avg": 90.0, "high_max": 80.0},
    "disk":       {"src": "disk_usage_percent", "critical_avg": 90.0, "high_max": 85.0},
    "net_sent":   {"src": "net_sent_mb",        "critical_avg": None, "high_max": None},
    "net_recv":   {"src": "net_recv_mb",        "critical_avg": None, "high_max": None},
    "disk_read":  {"src": "disk_read_mb",       "critical_avg": None, "high_max": None},
    "disk_write": {"src": "disk_write_mb",      "critical_avg": None, "high_max": None},
    "procs":      {"src": "process_count",      "critical_avg": None, "high_max": None},
}


def run_pipeline():
    print("Demarrage du moteur Spark Structured Streaming AIOps...")

    # =========================================================================
    # A. LECTURE KAFKA
    # =========================================================================
    df_metrics_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "aiops-metrics") \
        .option("startingOffsets", "latest") \
        .load()

    df_logs_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "aiops-logs") \
        .option("startingOffsets", "latest") \
        .load()

    # =========================================================================
    # B. BRONZE
    # =========================================================================
    query_bronze_metrics = df_metrics_kafka.selectExpr("CAST(value AS STRING) as raw_json") \
        .writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/app/storage/checkpoints/bronze_metrics") \
        .start("/app/storage/bronze/metrics")

    query_bronze_logs = df_logs_kafka.selectExpr("CAST(value AS STRING) as raw_json") \
        .writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", "/app/storage/checkpoints/bronze_logs") \
        .start("/app/storage/bronze/logs")

    # =========================================================================
    # C. SILVER METRICS : fenetre glissante + Z-score pour TOUTES les metriques
    # =========================================================================
    parsed_metrics = df_metrics_kafka \
        .select(from_json(col("value").cast("string"), metrics_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Agregations generees pour chaque metrique : avg, std, max
    aggs = []
    for m, cfg in METRICS.items():
        aggs += [
            avg(cfg["src"]).alias(f"{m}_avg"),
            stddev(cfg["src"]).alias(f"{m}_std"),
            spark_max(cfg["src"]).alias(f"{m}_max"),
        ]

    metrics_windowed = parsed_metrics \
        .withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "5 minutes", "10 seconds")) \
        .agg(*aggs)

    silver_metrics = metrics_windowed
    anomaly_flags = []

    for m, cfg in METRICS.items():
        avg_c, std_c, max_c = col(f"{m}_avg"), col(f"{m}_std"), col(f"{m}_max")

        # Z-score du pic : (max - moyenne) / ecart-type
        silver_metrics = silver_metrics.withColumn(
            f"z_{m}",
            when(std_c > 0, (max_c - avg_c) / std_c).otherwise(lit(0.0))
        )

        z_high = spark_abs(col(f"z_{m}")) > Z_THRESHOLD

        if cfg["critical_avg"] is not None:
            # Metriques avec seuils absolus (CPU, RAM, disque)
            silver_metrics = silver_metrics.withColumn(
                f"is_{m}_critical", avg_c > cfg["critical_avg"]
            )
            anomaly = (z_high & (max_c > cfg["high_max"])) | col(f"is_{m}_critical")
        else:
            # Metriques sans seuil absolu : Z eleve ET pic >= 2x la moyenne
            silver_metrics = silver_metrics.withColumn(f"is_{m}_critical", lit(False))
            anomaly = z_high & (max_c > avg_c * 2)

        silver_metrics = silver_metrics.withColumn(f"anomaly_{m}", anomaly)
        anomaly_flags.append(col(f"anomaly_{m}"))

    # Anomalie globale : au moins une metrique en anomalie
    silver_metrics = silver_metrics.withColumn(
        "is_anomaly", reduce(lambda a, b: a | b, anomaly_flags)
    )

    # Aplatir la fenetre pour Grafana (plus de colonne struct "window")
    silver_metrics = silver_metrics \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    query_silver_metrics = silver_metrics.writeStream \
        .format("delta").outputMode("complete") \
        .option("checkpointLocation", "/app/storage/checkpoints/silver_metrics") \
        .start("/app/storage/silver/metrics")

    # =========================================================================
    # SILVER LOGS
    # =========================================================================
    parsed_logs = df_logs_kafka \
        .select(from_json(col("value").cast("string"), logs_schema).alias("data")) \
        .select("data.*") \
        .withColumn("is_error", (col("status_code") >= 400) | (col("latency_ms") > 1000))

    query_silver_logs = parsed_logs.writeStream \
        .format("delta").outputMode("append") \
        .option("checkpointLocation", "/app/storage/checkpoints/silver_logs") \
        .start("/app/storage/silver/logs")

    # =========================================================================
    # D. GOLD
    # =========================================================================
    gold_incidents = parsed_logs.filter(col("is_error") == True)

    query_gold_incidents = gold_incidents.writeStream \
        .format("delta").outputMode("append") \
        .option("checkpointLocation", "/app/storage/checkpoints/gold_incidents") \
        .start("/app/storage/gold/incidents")

    print("Toutes les requetes streaming (Bronze, Silver, Gold) sont actives.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_pipeline()