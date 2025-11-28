#!/bin/bash
# Wait for Flink JobManager to be ready
echo "Waiting for Flink JobManager to be ready..."
until curl -s http://jobmanager:8081/overview > /dev/null; do
  echo "JobManager not ready yet, waiting..."
  sleep 2
done

echo "JobManager is ready! Submitting fraud detection job..."

# Submit the PyFlink job
flink run \
  -m jobmanager:8081 \
  -py /opt/flink/usrlib/fraud_detection_job.py \
  -j /opt/flink/lib/flink-dist*.jar

echo "Job submitted successfully!"



