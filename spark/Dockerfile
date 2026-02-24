FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Kafka connector JARs
RUN curl -fsSL -o /opt/bitnami/spark/jars/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar || true
RUN curl -fsSL -o /opt/bitnami/spark/jars/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar || true

COPY fraud_engine.py .
COPY latency_benchmark.py .

CMD ["python", "-u", "fraud_engine.py"]
