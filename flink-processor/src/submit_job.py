#!/usr/bin/env python3
"""Flink job submission script with health checks and classpath configuration."""
import subprocess
import time
import requests
import sys
import os
from pathlib import Path
from typing import Optional

# Configuration constants
JOBMANAGER_URL = "http://jobmanager:8081/v1/overview"
HEALTH_CHECK_MAX_RETRIES = 30
HEALTH_CHECK_TIMEOUT = 2
HEALTH_CHECK_INTERVAL = 2
JOB_FILE = "/opt/flink/usrlib/news_processing_job.py"
FLINK_LIB_DIR = Path("/opt/flink/lib")


def _wait_for_endpoint(
    endpoint_url: str,
    check_fn,
    service_name: str,
    max_retries: int = HEALTH_CHECK_MAX_RETRIES,
    timeout: float = HEALTH_CHECK_TIMEOUT,
    interval: float = HEALTH_CHECK_INTERVAL
) -> bool:
    """Generic function to wait for an endpoint to be ready with a custom check."""
    for attempt in range(max_retries):
        try:
            response = requests.get(endpoint_url, timeout=timeout)
            if response.status_code == 200:
                if check_fn(response):
                    return True
        except requests.exceptions.ConnectionError:
            pass
        except Exception as exc:
            print(f"  Error checking {service_name} status: {exc}")
        
        if attempt < max_retries - 1:
            print(f"  Attempt {attempt + 1}/{max_retries}: {service_name} not ready...")
            time.sleep(interval)
    
    print(f"{service_name} did not become ready in time")
    return False


def wait_for_jobmanager() -> bool:
    """Wait for JobManager to be ready."""
    def check_jobmanager(response: requests.Response) -> bool:
        print("JobManager is ready!")
        return True
    
    return _wait_for_endpoint(
        JOBMANAGER_URL,
        check_jobmanager,
        "JobManager"
    )


def wait_for_taskmanager() -> bool:
    """Wait for at least one TaskManager to register."""
    def check_taskmanager(response: requests.Response) -> bool:
        data = response.json()
        num_taskmanagers = data.get("taskmanagers", 0)
        if num_taskmanagers > 0:
            print(f"{num_taskmanagers} TaskManager(s) registered!")
            return True
        return False
    
    return _wait_for_endpoint(
        JOBMANAGER_URL,
        check_taskmanager,
        "TaskManager"
    )


def setup_classpath() -> bool:
    """Ensure Kafka connector JAR is in classpath and configure FLINK_CLASSPATH."""
    if not FLINK_LIB_DIR.exists():
        print(f"ERROR: Flink lib directory not found: {FLINK_LIB_DIR}")
        return False
    
    kafka_jars = list(FLINK_LIB_DIR.glob("flink-connector-kafka*.jar"))
    if not kafka_jars:
        print("ERROR: No Kafka connector JARs found!")
        return False
    
    print(f"Found Kafka connector JARs: {[j.name for j in kafka_jars]}")
    
    # Set FLINK_CLASSPATH to include all JARs
    all_jars = list(FLINK_LIB_DIR.glob("*.jar"))
    classpath = ":".join(str(jar) for jar in all_jars)
    os.environ["FLINK_CLASSPATH"] = classpath
    os.environ["FLINK_LIB_DIR"] = str(FLINK_LIB_DIR)
    
    print(f"FLINK_CLASSPATH set with {len(all_jars)} JARs")
    return True


def submit_job() -> bool:
    """Submit the Flink job to the JobManager."""
    if not Path(JOB_FILE).exists():
        print(f"ERROR: Job file not found: {JOB_FILE}")
        return False
    
    print(f"\nSubmitting job from {JOB_FILE}")
    
    env = os.environ.copy()
    env["JAVA_TOOL_OPTIONS"] = (
        "-Dlog4j.configuration=file:///opt/flink/conf/log4j.properties "
        "-Dlogback.configurationFile=/opt/flink/conf/logback.xml"
    )
    
    cmd = [
        "flink",
        "run",
        "-m", "jobmanager:8081",
        "-py", JOB_FILE
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print("-" * 60)
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    if result.stdout:
        print("STDOUT:\n", result.stdout)
    if result.stderr:
        print("STDERR:\n", result.stderr)
    
    print("-" * 60)
    print(f"Return code: {result.returncode}")
    
    if result.returncode == 0:
        print("Job submitted successfully!")
        return True
    else:
        print("Job submission failed!")
        return False


def main():
    """Main entry point for job submission."""
    print("=" * 60)
    print("FLINK JOB SUBMISSION")
    print("=" * 60)
    
    if not wait_for_jobmanager():
        sys.exit(1)
    
    if not wait_for_taskmanager():
        sys.exit(1)
    
    if not setup_classpath():
        sys.exit(1)
    
    print()
    if not submit_job():
        sys.exit(1)
    
    print("\nAll checks passed!")


if __name__ == "__main__":
    main()
