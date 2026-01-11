# News Processing Pipeline

A real-time news processing system that matches news articles to prediction markets using semantic similarity. Built with Apache Flink, Kafka, and PostgreSQL with pgvector.

## What It Does

The pipeline reads news articles from Kafka, generates embeddings for headlines, and matches them to prediction markets using cosine similarity. Only news that matches above a similarity threshold gets forwarded to the output topic.

**Flow:**
1. News articles arrive in Kafka (`news_stream` topic)
2. Flink processes each article:
   - Generates embeddings via embedding service
   - Matches to markets using pgvector similarity search
   - Stores all news in PostgreSQL database
3. Only matched news is published to `processed_news` topic

## Getting Started

Start everything:
```bash
docker compose up -d
```

Check status:
```bash
docker compose ps
curl http://localhost:8081/jobs
```

View logs:
```bash
docker compose logs -f jobmanager taskmanager
```

Stop everything:
```bash
docker compose down
```

## Configuration

Key environment variables (set in `docker-compose.yml`):

- `KAFKA_SOURCE_TOPIC`: Input topic (default: `news_stream`)
- `KAFKA_SINK_TOPIC`: Output topic (default: `processed_news`)
- `EMBEDDING_SERVICE_URL`: Embedding service endpoint
- `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`: Database connection
- `MIN_SIMILARITY_THRESHOLD`: Minimum similarity score to match (default: 0.5)

## Project Structure

```
flink-processor/
  ├── src/
  │   ├── news_processing_job.py  # Main Flink job
  │   └── submit_job.py            # Job submission script
  ├── Dockerfile                   # Flink + PyFlink setup
  └── flink-conf.yaml             # Flink cluster config
```

## Monitoring

Check Kafka topics:
```bash
# View message counts
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic news_stream processed_news

# Consume messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic processed_news \
  --from-beginning
```

Flink web UI:
```bash
open http://localhost:8081
```

## Troubleshooting

**JobManager keeps restarting?**
- Check `flink-conf.yaml` has memory settings configured
- Verify config file is mounted: `docker compose exec jobmanager cat /opt/flink/conf/flink-conf.yaml`

**No data flowing?**
- Check logs: `docker compose logs taskmanager | grep -i error`
- Verify Kafka topics exist: `docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092`
- Check embedding service is healthy: `curl http://localhost:8001/health`

**Python errors?**
- Check syntax: `docker compose exec taskmanager python3 -m py_compile /opt/flink/usrlib/news_processing_job.py`
- Check imports: `docker compose logs taskmanager | grep Traceback`

## Technologies

- **Apache Flink 1.18.1**: Stream processing engine
- **PyFlink**: Python API for Flink
- **Apache Kafka**: Message streaming
- **PostgreSQL + pgvector**: Vector similarity search
- **Docker Compose**: Container orchestration
