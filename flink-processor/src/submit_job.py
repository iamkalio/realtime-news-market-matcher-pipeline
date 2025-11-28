#!/usr/bin/env python3
"""
Flink job submission script with classpath debugging
"""
import subprocess
import time
import requests
import sys
import os
from pathlib import Path


def wait_for_jobmanager(max_retries=30, timeout=2):
    """Wait for JobManager to be ready"""
    for attempt in range(max_retries):
        try:
            response = requests.get("http://jobmanager:8081/v1/overview", timeout=timeout)
            if response.status_code == 200:
                print("✓ JobManager is ready!")
                return True
        except requests.exceptions.ConnectionError:
            print(f"  Attempt {attempt + 1}/{max_retries}: JobManager not ready...")
            time.sleep(2)
    
    print("✗ JobManager did not become ready in time")
    return False


def wait_for_taskmanager(max_retries=30, timeout=2):
    """Wait for at least one TaskManager to register"""
    for attempt in range(max_retries):
        try:
            response = requests.get("http://jobmanager:8081/v1/overview", timeout=timeout)
            if response.status_code == 200:
                data = response.json()
                num_taskmanagers = data.get("taskmanagers", 0)
                if num_taskmanagers > 0:
                    print(f"✓ {num_taskmanagers} TaskManager(s) registered!")
                    return True
            print(f"  Attempt {attempt + 1}/{max_retries}: Waiting for TaskManager registration...")
        except Exception as e:
            print(f"  Error checking TaskManager status: {e}")
        
        time.sleep(2)
    
    print("✗ No TaskManagers registered in time")
    return False


def setup_classpath():
    """Ensure Kafka connector JAR is in classpath"""
    lib_dir = Path("/opt/flink/lib")
    
    kafka_jars = list(lib_dir.glob("flink-connector-kafka*.jar"))
    if not kafka_jars:
        print("✗ ERROR: No Kafka connector JARs found!")
        return False
    
    print(f"✓ Found Kafka connector JARs: {[j.name for j in kafka_jars]}")
    
    # Set FLINK_CLASSPATH to include all JARs
    classpath = ":".join(str(jar) for jar in lib_dir.glob("*.jar"))
    os.environ["FLINK_CLASSPATH"] = classpath
    os.environ["FLINK_LIB_DIR"] = str(lib_dir)
    
    print(f"✓ FLINK_CLASSPATH set with {len(list(lib_dir.glob('*.jar')))} JARs")
    return True


def submit_job():
    """Submit the Flink job"""
    job_file = "/opt/flink/usrlib/fraud_detection_job.py"
    
    if not Path(job_file).exists():
        print(f"✗ Job file not found: {job_file}")
        return False
    
    print(f"\n→ Submitting job from {job_file}")
    
    env = os.environ.copy()
    env["JAVA_TOOL_OPTIONS"] = (
        "-Dlog4j.configuration=file:///opt/flink/conf/log4j.properties "
        "-Dlogback.configurationFile=/opt/flink/conf/logback.xml"
    )
    
    cmd = [
        "flink",
        "run",
        "-m", "jobmanager:8081",
        "-py", job_file
    ]
    
    print(f"→ Running: {' '.join(cmd)}")
    print("-" * 60)
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    
    if result.stdout:
        print("STDOUT:\n", result.stdout)
    if result.stderr:
        print("STDERR:\n", result.stderr)
    
    print("-" * 60)
    print(f"Return code: {result.returncode}")
    
    if result.returncode == 0:
        print("✓ Job submitted successfully!")
        return True
    else:
        print("✗ Job submission failed!")
        return False


def main():
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
    
    print("\n✓ All checks passed!")


if __name__ == "__main__":
    main()
