FROM apache/spark:4.2.0-python3

USER root

# ============================================================
# 1. CORE PYTHON DEPENDENCIES
# ============================================================
RUN pip install --no-cache-dir python-dotenv boto3

# ============================================================
# 2. MYSQL JDBC & AWS S3 CONNECTOR JARS
# ============================================================
RUN wget -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.4.0/mysql-connector-j-9.4.0.jar \
    -O /opt/spark/jars/mysql-connector-j-9.4.0.jar


# ============================================================
# AWS S3 / S3A SUPPORT
# Hadoop version bundled with Spark = 3.5.0
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
# 3. WORKDIR & STORAGE DIRECTORIES
# ============================================================
WORKDIR /opt/retailhub

RUN mkdir -p /opt/retailhub/data \
             /opt/retailhub/checkpoints \
             /opt/retailhub/logs

# ============================================================
# 4. PYTHON DEPENDENCIES
# ============================================================
COPY requirements.txt /opt/retailhub/requirements.txt

# Remove pyspark and py4j (already built into the base image) & set 1000s timeout
RUN sed -i '/pyspark/d; /py4j/d' /opt/retailhub/requirements.txt && \
    pip install --default-timeout=1000 --no-cache-dir \
    -r /opt/retailhub/requirements.txt

# ============================================================
# 5. SOURCE CODE & FRAUDGUARD
# ============================================================
COPY src /opt/retailhub/src
COPY fraudguard /opt/retailhub/fraudguard

RUN chown -R spark:spark /opt/retailhub

# ============================================================
# 6. RUNTIME ENVIRONMENT
# ============================================================
ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH="/opt/retailhub:/opt/retailhub/src"

USER spark