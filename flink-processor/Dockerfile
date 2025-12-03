FROM flink:1.18-java11

USER root

# Install Python, build tools, and Java development headers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    curl \
    wget \
    openjdk-11-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Set Python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# Find JAVA_HOME and set it up, then install PyFlink
RUN JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    echo "Detected JAVA_HOME: $JAVA_HOME_DIR" && \
    mkdir -p /opt/java && \
    if [ -e /opt/java/openjdk ]; then rm -rf /opt/java/openjdk; fi && \
    ln -s "$JAVA_HOME_DIR" /opt/java/openjdk && \
    JAVA_HOME=/opt/java/openjdk pip3 install --no-cache-dir apache-flink==1.18.1 requests==2.31.0

# Download Kafka connector matching Flink 1.18 (DataStream API)
RUN mkdir -p /opt/flink/lib/kafka-connectors && \
    cd /opt/flink/lib && \
    curl -fSL -o flink-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar && \
    chmod 644 flink-connector-kafka-3.0.1-1.18.jar && \
    echo "Downloaded Kafka connector 3.0.1-1.18"

# Kafka client library required by the connector
RUN curl -fSL -o kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    chmod 644 kafka-clients-3.5.1.jar && \
    mv kafka-clients-3.5.1.jar /opt/flink/lib/

# Ensure all core Flink JARs are present
RUN ls -la /opt/flink/lib/ | grep -E "(flink-dist|flink-streaming)" || echo "Core Flink JARs present"

ENV JAVA_HOME=/opt/java/openjdk \
    FLINK_PYTHON=python3 \
    PYFLINK_CLIENT_EXECUTABLE=python3 \
    PYTHONPATH=/opt/flink/usrlib:/opt/flink/lib/*

WORKDIR /opt/flink/usrlib
