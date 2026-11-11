FROM python:3.10-slim

WORKDIR /app

# Java + outils
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Téléchargement des JARs Kafka + Delta DANS le dossier jars de PySpark (au build)
RUN set -eux; \
    JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    M=https://repo1.maven.org/maven2; \
    cd "$JARS_DIR"; \
    curl -fsSLO $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar; \
    curl -fsSLO $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar; \
    curl -fsSLO $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar; \
    curl -fsSLO $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    curl -fsSLO $M/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar; \
    curl -fsSLO $M/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

COPY . .

CMD ["python", "processor.py"]