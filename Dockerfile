FROM apache/airflow:2.10.0

USER root

# 1. Install System Dependencies + Java
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    gcc \
    g++ \
    curl \
    openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Install Spark Binaries (Required for SparkSubmitOperator)
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN curl -O https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && tar -xvzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Set Environment Paths
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="$PATH:$SPARK_HOME/bin"

USER airflow

# 3. Install Python packages
# Note: Added the airflow-spark provider
RUN pip install --no-cache-dir \
    boto3==1.34.97 \
    duckdb==1.0.0 \
    streamlit==1.32.0 \
    pyspark==3.5.0 \
    spacy==3.7.4 \
    requests==2.31.0 \
    beautifulsoup4==4.12.3 \
    apache-airflow-providers-apache-spark==5.0.0

# 4. Download spaCy model
RUN python -m spacy download en_core_web_sm