# Kafka → Flink → Kafka Pipeline Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Technology Stack & Rationale](#technology-stack--rationale)
4. [Project Structure](#project-structure)
5. [Build Process & Challenges](#build-process--challenges)
6. [Error Resolution Journey](#error-resolution-journey)
7. [Configuration Deep Dive](#configuration-deep-dive)
8. [Debugging Guide](#debugging-guide)
9. [Verification & Monitoring](#verification--monitoring)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This project implements a **real-time fraud detection pipeline** using Apache Flink with PyFlink. The pipeline:

1. **Consumes** transaction data from Kafka (`transactions` topic)
2. **Processes** transactions in real-time to detect fraudulent patterns (amount > $5000)
3. **Produces** fraud alerts to another Kafka topic (`fraud-alerts`)

The entire system runs in Docker containers, making it portable and easy to deploy.

### Key Features
- ✅ Real-time stream processing with Apache Flink
- ✅ Python-based processing logic (PyFlink)
- ✅ Kafka integration for event streaming
- ✅ Dockerized deployment for easy setup
- ✅ Automatic topic creation
- ✅ Synthetic data generation for testing

---

## Architecture

### High-Level Flow

```
┌─────────────────┐
│ Kafka Producer  │  (Generates synthetic transactions)
└────────┬────────┘
         │
         │ Produces JSON transactions
         ▼
┌─────────────────┐
│  Kafka Broker   │  (transactions topic)
└────────┬────────┘
         │
         │ Consumes via FlinkKafkaConsumer
         ▼
┌─────────────────┐
│  Flink Job      │  (PyFlink processing)
│  - Parse JSON   │
│  - Filter fraud │  (amount > $5000)
│  - Serialize    │
└────────┬────────┘
         │
         │ Produces via FlinkKafkaProducer
         ▼
┌─────────────────┐
│  Kafka Broker   │  (fraud-alerts topic)
└─────────────────┘
```

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                            │
│                  (flink-pipeline-net)                        │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Zookeeper   │  │    Kafka     │  │ Kafka Setup  │     │
│  │   :2181      │◄─┤   :9092      │  │  (one-time)  │     │
│  └──────────────┘  └──────┬───────┘  └──────────────┘     │
│                            │                                 │
│  ┌─────────────────────────┼─────────────────────────┐     │
│  │                         │                         │     │
│  │  ┌──────────────┐  ┌────▼──────┐  ┌──────────┐   │     │
│  │  │  JobManager  │  │ TaskMgr   │  │ Producer │   │     │
│  │  │   :8081      │◄─┤  :6122     │  │          │   │     │
│  │  │   :6123      │  │            │  │          │   │     │
│  │  └──────────────┘  └────────────┘  └──────────┘   │     │
│  │         │                │              │          │     │
│  │         └────────────────┴──────────────┘          │     │
│  │                    (Kafka)                          │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌──────────────┐                                           │
│  │ Job Submitter│  (Submits PyFlink job)                    │
│  └──────────────┘                                           │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Producer** (`kafka-producer`) generates synthetic transaction JSON every 2 seconds
2. **Kafka** stores messages in `transactions` topic
3. **Flink JobManager** coordinates job execution
4. **Flink TaskManager** executes the PyFlink job:
   - Reads from `transactions` topic
   - Parses JSON strings
   - Filters transactions with `amount > 5000`
   - Serializes filtered results back to JSON strings
   - Writes to `fraud-alerts` topic
5. **Consumers** can read fraud alerts from `fraud-alerts` topic

---

## Technology Stack & Rationale

### Core Technologies

#### 1. **Apache Flink 1.18.1**
- **Why**: Industry-standard stream processing engine with excellent Python support
- **Version**: `1.18.1` - Stable release with PyFlink support
- **Image**: `flink:1.18-java11` - Official Docker image with Java 11

#### 2. **PyFlink (apache-flink==1.18.1)**
- **Why**: Allows writing Flink jobs in Python instead of Java/Scala
- **Version**: Must match Flink version exactly (`1.18.1`)
- **Dependencies**: Requires `pemja` (Python-Java bridge) which needs JDK headers

#### 3. **Apache Kafka (Confluent Platform 7.6.0)**
- **Why**: Industry-standard event streaming platform
- **Image**: `confluentinc/cp-kafka:7.6.0` - Production-ready Kafka distribution
- **Zookeeper**: Required for Kafka coordination (Confluent Platform still uses it)

#### 4. **Python 3.10+**
- **Why**: PyFlink requires Python 3.6+, we use 3.10 for better compatibility
- **Packages**:
  - `apache-flink==1.18.1`: PyFlink Python API
  - `requests`: Originally for Elasticsearch (removed later)
  - `faker`: Generate realistic test data
  - `kafka-python`: Python Kafka client for producer

### Docker Images Breakdown

| Component | Image | Purpose |
|-----------|-------|---------|
| Zookeeper | `confluentinc/cp-zookeeper:7.6.0` | Kafka coordination |
| Kafka | `confluentinc/cp-kafka:7.6.0` | Event streaming broker |
| Flink Base | `flink:1.18-java11` | Base for JobManager/TaskManager |
| Python | Built on Flink base | Custom image with PyFlink |

---

## Project Structure

```
poly-forcaster/
├── docker-compose.yml          # Orchestrates all services
├── ARCHITECTURE.md             # This file
│
├── kafka-producer/             # Synthetic transaction generator
│   ├── Dockerfile              # Python + Kafka client
│   ├── producer.py             # Main producer logic
│   ├── requirements.txt       # Python dependencies
│   └── wait-for-it.sh          # Waits for Kafka to be ready
│
└── flink-processor/            # Flink processing logic
    ├── Dockerfile              # Flink + PyFlink + Kafka connectors
    ├── flink-conf.yaml         # Flink cluster configuration
    ├── wait-for-it.sh          # Service readiness checks
    └── src/
        ├── fraud_detection_job.py  # Main PyFlink job
        └── submit_job.py           # Job submission script
```

---

## Build Process & Challenges

### Initial Goal
Create a simple Kafka → Flink → Elasticsearch pipeline using PyFlink.

### Evolution
1. **Started**: Kafka → Flink → Elasticsearch
2. **Simplified**: Kafka → Flink → Kafka (removed Elasticsearch due to complexity)

### Docker Build Challenges

#### Challenge 1: PyFlink Installation Requires JDK Headers

**Problem**: 
```bash
pip install apache-flink==1.18.1
# Error: Could not find /opt/java/openjdk/include
# pemja compilation failed
```

**Root Cause**: 
- PyFlink's `pemja` package (Python-Java bridge) needs to compile native extensions
- Requires JDK headers (`jni.h`, etc.) not present in JRE-only images
- Flink Docker image has JRE, not full JDK

**Solution**:
```dockerfile
# Install full JDK (not just JRE)
RUN apt-get install -y openjdk-11-jdk-headless

# Find actual JDK location
RUN JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" && \
    mkdir -p /opt/java && \
    if [ -e /opt/java/openjdk ]; then rm -rf /opt/java/openjdk; fi && \
    ln -s "$JAVA_HOME_DIR" /opt/java/openjdk && \
    JAVA_HOME=/opt/java/openjdk pip3 install apache-flink==1.18.1
```

**Why**: 
- `openjdk-11-jdk-headless` provides `javac` and headers
- Symlink to `/opt/java/openjdk` because `pemja` expects that exact path
- Set `JAVA_HOME` before `pip install` so compilation finds headers

#### Challenge 2: Python vs python3

**Problem**:
```bash
Cannot run program "python": error=2, No such file or directory
```

**Root Cause**: 
- Flink's Python runner looks for `python` command
- Ubuntu/Debian only has `python3` by default

**Solution**:
```dockerfile
RUN ln -s /usr/bin/python3 /usr/bin/python
```

#### Challenge 3: Kafka Connector JAR Dependencies

**Problem**: 
```bash
TypeError: Could not found the Java class 'org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer'
```

**Root Cause**: 
- PyFlink's `FlinkKafkaConsumer` is a Python wrapper around Java classes
- Java classes must be in Flink's classpath (`/opt/flink/lib/`)
- Flink base image doesn't include Kafka connectors

**Solution**:
```dockerfile
# Download Flink 1.18 compatible Kafka connector
RUN curl -fSL -o /opt/flink/lib/flink-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar

# Download Kafka client library (required dependency)
RUN curl -fSL -o /opt/flink/lib/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
```

**Why These Versions**:
- **Flink 1.18** uses connector version `3.0.1-1.18` (unified connector, no Scala suffix)
- **Kafka clients 3.5.1** is compatible with Confluent Platform 7.6.0
- Both must be in `/opt/flink/lib/` for Flink to find them

#### Challenge 4: Wrong Connector Artifact Names

**Initial Attempt**:
```dockerfile
# ❌ WRONG - This artifact doesn't exist
RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.18.1/flink-connector-kafka-1.18.1.jar
# Error: 404 Not Found
```

**Why Failed**:
- Flink 1.18 changed connector naming convention
- Old format: `flink-connector-kafka-{version}.jar`
- New format: `flink-connector-kafka-{connector-version}-{flink-version}.jar`

**Correct Version**:
```dockerfile
# ✅ CORRECT
RUN curl -fSL -o flink-connector-kafka-3.0.1-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.18/flink-connector-kafka-3.0.1-1.18.jar
```

#### Challenge 5: Missing Kafka Client Library

**Problem**:
```bash
java.lang.NoClassDefFoundError: org/apache/kafka/common/serialization/ByteArrayDeserializer
```

**Root Cause**: 
- `FlinkKafkaConsumer` uses Kafka's serialization classes
- These are in `kafka-clients` JAR, not in the connector JAR
- Connector JAR depends on `kafka-clients` but doesn't bundle it

**Solution**: Download `kafka-clients-3.5.1.jar` separately (see Challenge 3)

---

## Error Resolution Journey

### Error 1: JobManager Restart Loop

**Symptom**:
```bash
jobmanager-1 exited with code 1 (restarting)
```

**Error Message**:
```
IllegalConfigurationException: Either required fine-grained memory 
(jobmanager.memory.heap.size), or Total Flink Memory size 
(Key: 'jobmanager.memory.flink.size'), or Total Process Memory size 
(Key: 'jobmanager.memory.process.size') need to be configured explicitly.
```

**Root Cause**: 
- Flink 1.18 requires explicit memory configuration when using custom `flink-conf.yaml`
- Default memory calculation doesn't work with mounted config files

**Solution**: Added to `flink-conf.yaml`:
```yaml
jobmanager.memory.process.size: 1024m
taskmanager.memory.process.size: 1536m
taskmanager.memory.managed.size: 512m
```

**Why These Values**:
- Conservative for laptop/development
- `1024m` for JobManager (coordination only)
- `1536m` for TaskManager (executes tasks)
- `512m` managed memory (for RocksDB state, if used)

### Error 2: ClassCastException in Kafka Sink

**Symptom**:
```bash
java.lang.ClassCastException: class [B cannot be cast to class java.lang.String
at org.apache.flink.api.common.serialization.SimpleStringSchema.serialize
```

**Root Cause**: 
- PyFlink map operations without explicit type info return pickled byte arrays
- `SimpleStringSchema` expects Java `String` objects
- Type mismatch causes serialization failure

**Solution**: Added explicit type information:
```python
from pyflink.common.typeinfo import Types

fraud_alert_strings = fraud_alerts.map(
    lambda tx: json.dumps(tx),
    output_type=Types.STRING(),  # ← Critical: tells Flink to use String type
)
```

**Why This Works**: 
- `output_type=Types.STRING()` tells Flink's type system the output is a Java String
- Flink handles Python-to-Java conversion correctly
- `SimpleStringSchema` receives proper String objects

### Error 3: FLINK_PROPERTIES Format in docker-compose.yml

**Initial (Broken) Format**:
```yaml
environment:
  - |
    FLINK_PROPERTIES=
    jobmanager.rpc.address: jobmanager
```

**Problem**: 
- Docker Compose treats the entire block as a single string
- First line `FLINK_PROPERTIES=` is included in the value
- Flink can't parse malformed properties

**Solution**: Use proper YAML multiline format:
```yaml
environment:
  FLINK_PROPERTIES: |
    jobmanager.rpc.address: jobmanager
    parallelism.default: 1
    taskmanager.numberOfTaskSlots: 1
```

**Why**: 
- `FLINK_PROPERTIES: |` creates a multiline string
- No `=` on first line
- Proper indentation for YAML

### Error 4: Elasticsearch Integration Complexity

**Original Plan**: Send fraud alerts to Elasticsearch for search/analytics

**Challenges Encountered**:
1. PyFlink's `SinkFunction` interface is complex
2. `RichSinkFunction` not available in PyFlink API
3. Custom sink requires proper lifecycle management
4. HTTP requests from UDFs can cause backpressure issues

**Decision**: Removed Elasticsearch to keep pipeline simple and focused

**Alternative Approaches** (if needed later):
- Use Flink's Elasticsearch connector (Java-based)
- Send to Elasticsearch via Kafka (Kafka → Elasticsearch connector)
- Use Flink's HTTP sink (if available in future versions)

---

## Configuration Deep Dive

### docker-compose.yml Structure

#### Network Configuration
```yaml
networks:
  default:
    name: flink-pipeline-net
```
- All services on same network can communicate by service name
- `jobmanager`, `taskmanager`, `kafka` are DNS-resolvable

#### Volume Mounts
```yaml
volumes:
  - ./flink-processor/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml:ro
  - ./flink-processor/src:/opt/flink/usrlib:ro
```
- `:ro` = read-only (prevents Flink from modifying files)
- Config mounted so changes don't require rebuild
- Source code mounted for development iteration

#### Service Dependencies
```yaml
depends_on:
  kafka-setup:
    condition: service_completed_successfully
```
- Ensures topics exist before Flink starts
- Prevents race conditions

### flink-conf.yaml Explained

```yaml
# JobManager Configuration
jobmanager.rpc.address: jobmanager        # Hostname for RPC
jobmanager.rpc.port: 6123                  # RPC port
jobmanager.memory.process.size: 1024m      # Total memory (required in 1.18)

# TaskManager Configuration
taskmanager.host: taskmanager              # Hostname
taskmanager.memory.process.size: 1536m     # Total memory
taskmanager.memory.managed.size: 512m      # Managed memory (for state)
taskmanager.numberOfTaskSlots: 4           # Parallelism capacity

# Job Configuration
parallelism.default: 2                     # Default parallelism
python.executable: python3                  # Python interpreter

# State Backend
state.checkpoints.dir: file:///flink-checkpoints/  # Checkpoint storage
state.savepoints.dir: file:///flink-savepoints/    # Savepoint storage
```

**Why These Settings**:
- **Memory sizes**: Required by Flink 1.18 when using custom config
- **Task slots**: 4 slots allow 4 parallel tasks per TaskManager
- **Default parallelism**: 2 means 2 parallel instances of each operator
- **Python executable**: Explicit path ensures Flink finds Python

### Environment Variables

#### JobManager/TaskManager
```yaml
KAFKA_BOOTSTRAP_SERVERS: kafka:9092
KAFKA_SOURCE_TOPIC: transactions
KAFKA_SINK_TOPIC: fraud-alerts
FRAUD_THRESHOLD: "5000"
```
- Passed to Python job via `os.getenv()`
- Allows configuration without code changes

#### Kafka Producer
```yaml
KAFKA_BOOTSTRAP_SERVERS: kafka:9092
KAFKA_TOPIC: transactions
PRODUCER_INTERVAL_SECONDS: 2
```
- Controls producer behavior
- `PRODUCER_INTERVAL_SECONDS` controls data generation rate

---

## Debugging Guide

### 1. Verify Docker Services Are Running

```bash
# Check all container status
docker compose ps

# Expected output:
# NAME                          STATUS
# poly-forcaster-jobmanager-1   Up X minutes
# poly-forcaster-taskmanager-1  Up X minutes
# poly-forcaster-kafka-1        Up X minutes
# poly-forcaster-kafka-producer-1 Up X minutes
```

**Troubleshooting**:
- If containers are `Restarting`: Check logs with `docker compose logs <service>`
- If containers are `Exited`: Check exit code with `docker compose ps -a`

### 2. Check Flink Cluster Health

#### Via Web UI
```bash
# Open in browser
open http://localhost:8081
```

**What to Check**:
- **Overview**: Should show 1 TaskManager registered
- **Jobs**: Should show "Fraud Detection Job" in RUNNING state
- **Task Managers**: Should show 1 TaskManager with 4 slots

#### Via REST API
```bash
# Check running jobs
curl -s http://localhost:8081/jobs | jq .

# Check TaskManager status
curl -s http://localhost:8081/taskmanagers | jq .

# Get specific job details (replace JOB_ID)
curl -s http://localhost:8081/jobs/JOB_ID | jq .
```

### 3. Monitor Kafka Topics

#### Check Topic Offsets (Message Counts)
```bash
# See current offsets for all topics
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic transactions fraud-alerts
```

**Expected Output**:
```
transactions:0:1355
fraud-alerts:0:516
```
- Numbers should increase over time
- `fraud-alerts` should be ~30-40% of `transactions` (fraud rate)

#### Consume Messages in Real-Time

**Terminal 1 - Watch Transactions**:
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic transactions \
  --from-beginning
```

**Terminal 2 - Watch Fraud Alerts**:
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic fraud-alerts \
  --from-beginning
```

**Terminal 3 - Monitor Offsets**:
```bash
watch -n 2 'docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic transactions fraud-alerts'
```

### 4. Check Flink Logs

#### JobManager Logs
```bash
# Follow logs in real-time
docker compose logs -f jobmanager

# Last 100 lines
docker compose logs --tail=100 jobmanager

# Filter for errors
docker compose logs jobmanager | grep -i "error\|exception\|failed"
```

#### TaskManager Logs
```bash
# Follow logs
docker compose logs -f taskmanager

# Check for Python errors
docker compose logs taskmanager | grep -i "python\|traceback\|error"
```

**Key Log Messages to Watch For**:
- ✅ `Job switched from state CREATED to RUNNING` - Job started successfully
- ✅ `Consumer subtask 0 will start reading` - Kafka consumer connected
- ✅ `Starting FlinkKafkaInternalProducer` - Kafka producer initialized
- ❌ `Python process exits with code: 1` - Python job crashed
- ❌ `ClassNotFoundException` - Missing JAR dependency
- ❌ `TypeError` - Python type mismatch

### 5. Verify Kafka Connectivity

#### Test Producer
```bash
# Check producer logs
docker compose logs kafka-producer

# Should see:
# Produced transaction: {'transaction_id': '...', 'amount': 1234.56, ...}
```

#### Test Consumer (Manual)
```bash
# Consume from transactions topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic transactions \
  --max-messages 5
```

### 6. Check Python Job Execution

#### View Job Submission Logs
```bash
docker compose logs job-submitter

# Should see:
# ✓ JobManager is ready!
# ✓ 1 TaskManager(s) registered!
# ✓ Found Kafka connector JARs
# Job has been submitted with JobID ...
```

#### Test Python Code Directly (Advanced)
```bash
# Run Python job manually in container
docker compose exec taskmanager python3 /opt/flink/usrlib/fraud_detection_job.py
```

**Note**: This may fail without Flink cluster, but useful for syntax errors

### 7. Verify Network Connectivity

```bash
# Test JobManager → TaskManager
docker compose exec jobmanager ping -c 2 taskmanager

# Test TaskManager → Kafka
docker compose exec taskmanager ping -c 2 kafka

# Test Producer → Kafka
docker compose exec kafka-producer ping -c 2 kafka
```

### 8. Check Resource Usage

```bash
# Container resource usage
docker stats

# Check disk space
docker compose exec jobmanager df -h
docker compose exec taskmanager df -h
```

---

## Verification & Monitoring

### Quick Health Check Script

Create `verify.sh`:
```bash
#!/bin/bash

echo "=== Docker Services ==="
docker compose ps

echo -e "\n=== Flink Cluster ==="
curl -s http://localhost:8081/jobs | jq -r '.jobs[] | "\(.id) - \(.status)"'

echo -e "\n=== Kafka Topics ==="
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic transactions fraud-alerts

echo -e "\n=== Recent Fraud Alerts ==="
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic fraud-alerts \
  --max-messages 3 \
  --timeout-ms 5000
```

Run: `chmod +x verify.sh && ./verify.sh`

### Expected Healthy State

1. **All containers running**:
   ```
   jobmanager-1      Up X minutes
   taskmanager-1     Up X minutes
   kafka-1           Up X minutes
   kafka-producer-1  Up X minutes
   ```

2. **Flink job RUNNING**:
   ```json
   {
     "id": "cc0688a7dc31ff77688f567a4f3451ab",
     "status": "RUNNING"
   }
   ```

3. **Messages flowing**:
   ```
   transactions:0:1355    (increasing)
   fraud-alerts:0:516     (increasing, ~30-40% of transactions)
   ```

4. **No errors in logs**:
   ```bash
   docker compose logs jobmanager taskmanager | grep -i error
   # Should return nothing or only harmless warnings
   ```

### Monitoring Commands Reference

| What to Monitor | Command |
|----------------|---------|
| Container status | `docker compose ps` |
| Flink jobs | `curl -s http://localhost:8081/jobs \| jq .` |
| Kafka offsets | `docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic transactions fraud-alerts` |
| JobManager logs | `docker compose logs -f jobmanager` |
| TaskManager logs | `docker compose logs -f taskmanager` |
| Producer output | `docker compose logs -f kafka-producer` |
| Consume transactions | `docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic transactions` |
| Consume alerts | `docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic fraud-alerts` |

---

## Troubleshooting

### Problem: JobManager Keeps Restarting

**Symptoms**:
- Container status: `Restarting`
- Logs show memory configuration errors

**Solution**:
1. Check `flink-conf.yaml` has memory settings:
   ```yaml
   jobmanager.memory.process.size: 1024m
   taskmanager.memory.process.size: 1536m
   ```
2. Verify file is mounted correctly:
   ```bash
   docker compose exec jobmanager cat /opt/flink/conf/flink-conf.yaml
   ```

### Problem: "Could not found the Java class FlinkKafkaConsumer"

**Symptoms**:
- Job fails immediately
- Error: `TypeError: Could not found the Java class`

**Solution**:
1. Verify Kafka connector JAR exists:
   ```bash
   docker compose exec jobmanager ls -la /opt/flink/lib/ | grep kafka
   ```
2. Should see:
   - `flink-connector-kafka-3.0.1-1.18.jar`
   - `kafka-clients-3.5.1.jar`
3. If missing, rebuild image:
   ```bash
   docker compose build jobmanager taskmanager
   ```

### Problem: "ClassCastException: class [B cannot be cast to class java.lang.String"

**Symptoms**:
- Job starts but crashes when processing
- Error in Kafka sink serialization

**Solution**:
1. Ensure map operations have explicit type:
   ```python
   fraud_alert_strings = fraud_alerts.map(
       lambda tx: json.dumps(tx),
       output_type=Types.STRING(),  # ← Must include this
   )
   ```
2. Import required:
   ```python
   from pyflink.common.typeinfo import Types
   ```

### Problem: No Data Flowing Through Pipeline

**Symptoms**:
- Job is RUNNING
- Kafka topics have messages
- But fraud-alerts not increasing

**Diagnosis Steps**:
1. Check consumer group offset:
   ```bash
   docker compose exec kafka kafka-consumer-groups \
     --bootstrap-server kafka:9092 \
     --group flink-fraud-detection \
     --describe
   ```
2. Check TaskManager logs for Kafka consumer activity:
   ```bash
   docker compose logs taskmanager | grep -i "kafka\|consumer\|partition"
   ```
3. Verify filter logic is correct (check threshold):
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server kafka:9092 \
     --topic transactions \
     --max-messages 10 | jq '.amount'
   # Check if any amounts > 5000
   ```

### Problem: "Node -1 disconnected" Warnings

**Symptoms**:
- Logs show: `Node -1 disconnected`
- But job continues running

**Explanation**: 
- **This is NORMAL** - not an error
- "Node -1" is Kafka's metadata node
- Happens during metadata refresh (every 5 minutes)
- Can be ignored unless job actually stops

**Verify it's harmless**:
```bash
# Check job is still running
curl -s http://localhost:8081/jobs | jq -r '.jobs[].status'
# Should return: RUNNING

# Check messages are still flowing
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic fraud-alerts
# Number should be increasing
```

### Problem: Python Process Exits with Code 1

**Symptoms**:
- JobManager logs: `Python process exits with code: 1`
- Job fails immediately

**Diagnosis**:
1. Check Python syntax:
   ```bash
   docker compose exec taskmanager python3 -m py_compile /opt/flink/usrlib/fraud_detection_job.py
   ```
2. Check imports:
   ```bash
   docker compose exec taskmanager python3 -c "from pyflink.datastream import StreamExecutionEnvironment; print('OK')"
   ```
3. Check full error in TaskManager logs:
   ```bash
   docker compose logs taskmanager | grep -A 20 "Traceback\|Error\|Exception"
   ```

### Problem: Container Can't Connect to Kafka

**Symptoms**:
- Connection refused errors
- Timeout errors

**Diagnosis**:
1. Verify Kafka is running:
   ```bash
   docker compose ps kafka
   ```
2. Test connectivity:
   ```bash
   docker compose exec jobmanager ping -c 2 kafka
   docker compose exec jobmanager nc -zv kafka 9092
   ```
3. Check network:
   ```bash
   docker network inspect flink-pipeline-net | grep -A 5 "Containers"
   ```

### Problem: Build Fails with "Device or resource busy"

**Symptoms**:
- Docker build fails
- Error about file system

**Solution**:
- Usually means container is running and using the file
- Stop containers first:
  ```bash
  docker compose down
  docker compose build
  ```

### Problem: Flink UI Shows 0 Records/Bytes

**Symptoms**:
- Web UI shows job RUNNING
- But all counters are 0

**Explanation**:
- **This is a known PyFlink limitation**
- Python operators don't always report metrics correctly
- **Data is still flowing** - verify via Kafka offsets

**Verification**:
```bash
# Check Kafka offsets are increasing
watch -n 2 'docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic fraud-alerts'
```

---

## Key Learnings & Best Practices

### 1. PyFlink Type System
- **Always specify `output_type`** for map operations that feed into sinks
- Use `Types.STRING()` for string outputs
- Prevents `ClassCastException` at serialization

### 2. Flink 1.18 Memory Configuration
- **Must specify** `jobmanager.memory.process.size` and `taskmanager.memory.process.size`
- Required when using custom `flink-conf.yaml`
- Use conservative values for development (1-2GB)

### 3. Kafka Connector Versions
- Flink 1.18 uses connector version `3.0.1-1.18` (not `1.18.1`)
- Always download `kafka-clients` JAR separately
- Both must be in `/opt/flink/lib/`

### 4. Docker Compose Environment Variables
- Use `KEY: value` format, not `- KEY=value` in lists
- Multiline values use `|` or `>` syntax
- No `=` on first line of multiline values

### 5. Python Symlink
- Flink expects `python` command
- Create symlink: `ln -s /usr/bin/python3 /usr/bin/python`
- Do this before installing PyFlink

### 6. Build Order
- Install JDK → Set JAVA_HOME → Install PyFlink → Download JARs
- Each step depends on previous
- Test each layer before moving to next

### 7. Debugging Strategy
1. **Check container status first** (`docker compose ps`)
2. **Check logs for errors** (`docker compose logs`)
3. **Verify connectivity** (ping, curl)
4. **Check data flow** (Kafka offsets)
5. **Test components individually** (producer, consumer, Flink)

---

## Performance Tuning (Future)

### Current Configuration
- **Parallelism**: 2 (default)
- **Task Slots**: 4 per TaskManager
- **Memory**: Conservative (1GB JobManager, 1.5GB TaskManager)

### Optimization Opportunities

1. **Increase Parallelism**:
   ```yaml
   parallelism.default: 4  # More parallel processing
   ```

2. **Adjust Memory** (if needed):
   ```yaml
   taskmanager.memory.process.size: 2048m  # More memory for larger workloads
   ```

3. **Enable Checkpointing** (for fault tolerance):
   ```yaml
   execution.checkpointing.interval: 60000  # 1 minute
   ```

4. **Kafka Consumer Settings**:
   ```python
   kafka_props = {
       "bootstrap.servers": KAFKA_BOOTSTRAP,
       "group.id": "flink-fraud-detection",
       "auto.offset.reset": "earliest",
       "max.poll.records": "1000",  # Process more records per poll
   }
   ```

---

## Conclusion

This pipeline demonstrates a complete real-time stream processing setup using:
- **Apache Flink** for stream processing
- **PyFlink** for Python-based logic
- **Apache Kafka** for event streaming
- **Docker** for containerization

The journey involved solving multiple challenges around:
- PyFlink installation and compilation
- Kafka connector dependencies
- Flink memory configuration
- Python type system integration
- Docker networking and volumes

The final architecture is **simple, functional, and production-ready** for development and testing scenarios.

---

## Quick Reference Commands

### Start Everything
```bash
docker compose up -d
```

### Stop Everything
```bash
docker compose down
```

### Rebuild After Code Changes
```bash
docker compose build jobmanager taskmanager
docker compose up -d jobmanager taskmanager
```

### View Logs
```bash
docker compose logs -f jobmanager taskmanager
```

### Check Status
```bash
docker compose ps
curl -s http://localhost:8081/jobs | jq .
```

### Monitor Data Flow
```bash
# Terminal 1: Transactions
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic transactions

# Terminal 2: Fraud Alerts
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic fraud-alerts

# Terminal 3: Offsets
watch -n 2 'docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic transactions fraud-alerts'
```

---

**Last Updated**: 2025-11-28  
**Flink Version**: 1.18.1  
**Kafka Version**: 7.6.0 (Confluent Platform)  
**Python Version**: 3.10

