FROM apache/spark:4.2.0-python3

USER root

# ============================================================
# 1. CORE PYTHON DEPENDENCIES
# ============================================================
RUN pip install --no-cache-dir python-dotenv boto3

# ============================================================
# 2. SPARK KAFKA CONNECTOR JARS
# ============================================================
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar \
    -O /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.2.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar \
    -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.2.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar \
    -O /opt/spark/jars/kafka-clients-3.9.2.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar \
    -O /opt/spark/jars/commons-pool2-2.13.1.jar

# ============================================================
# 3. AWS S3 HADOOP CONNECTOR JARS (S3A SUPPORT)
# Hadoop version bundled with Spark image = 3.5.0
# ============================================================
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
    -O /opt/spark/jars/hadoop-aws-3.5.0.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
    -O /opt/spark/jars/bundle-2.35.4.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
    -O /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/2.2.5.Final/wildfly-openssl-2.2.5.Final.jar \
    -O /opt/spark/jars/wildfly-openssl-2.2.5.Final.jar

# ============================================================
# 4. WORKDIR & STORAGE DIRECTORIES
# ============================================================
WORKDIR /opt/retailhub

RUN mkdir -p /opt/retailhub/data \
             /opt/retailhub/checkpoints \
             /opt/retailhub/logs

# ============================================================
# 5. PYTHON DEPENDENCIES (GEMINI AI + KAFKA PRODUCER)
# ============================================================
COPY requirements.txt /opt/retailhub/requirements.txt

# Remove pyspark and py4j (already built into the base image) & set 1000s timeout
RUN sed -i '/pyspark/d; /py4j/d' /opt/retailhub/requirements.txt && \
    pip install --default-timeout=1000 --no-cache-dir \
    -r /opt/retailhub/requirements.txt \
    kafka-python-ng

    
# ============================================================
# 6. SOURCE CODE & FRAUDGUARD MODULES
# ============================================================
COPY src /opt/retailhub/src
COPY fraudguard /opt/retailhub/fraudguard

RUN chown -R spark:spark /opt/retailhub

# ============================================================
# 7. RUNTIME ENVIRONMENT
# ============================================================
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH="/opt/retailhub:/opt/retailhub/src"

USER spark