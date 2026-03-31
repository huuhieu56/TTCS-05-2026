FROM apache/airflow:2.10.4-python3.11

# ── System packages (JDK for PySpark/JDBC) ──────────────────────
USER root
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── Ivy cache for Spark dependency downloads ─────────────────────
RUN mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars \
    && chmod -R 777 /home/airflow/.ivy2

# ── Python packages ─────────────────────────────────────────────
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
