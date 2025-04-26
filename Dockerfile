# Start from official Python 3.11 image
FROM python:3.11

# Install Java (needed for Spark) and wget
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=${SPARK_HOME}/python
ENV PATH=$PATH:${SPARK_HOME}/bin

# Download and install Spark 3.5.2
RUN wget https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.2-bin-hadoop3.tgz && \
    mv spark-3.5.2-bin-hadoop3 /opt/spark && \
    rm spark-3.5.2-bin-hadoop3.tgz

# Install Python dependencies
RUN pip install pyspark==3.5.2 jupyterlab

# Default command
CMD ["bash"]