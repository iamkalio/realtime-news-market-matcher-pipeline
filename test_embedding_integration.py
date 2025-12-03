#!/usr/bin/env python3
"""
Test script to verify embedding service integration.
Run this after starting the services with: docker compose up -d
"""

import requests
import time

EMBEDDING_SERVICE_URL = "http://localhost:8001"

def test_health():
    """Test health endpoint."""
    print("Testing health endpoint...")
    try:
        response = requests.get(f"{EMBEDDING_SERVICE_URL}/health")
        if response.status_code == 200:
            print("✓ Health check passed")
            return True
        else:
            print(f"✗ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Health check error: {e}")
        return False


def test_single_embedding():
    """Test single embedding generation."""
    print("\nTesting single embedding...")
    try:
        response = requests.post(
            f"{EMBEDDING_SERVICE_URL}/embed",
            json={"text": "Fed raises interest rates again"}
        )
        if response.status_code == 200:
            data = response.json()
            embedding = data.get("embedding")
            dimension = data.get("dimension")
            print(f"✓ Single embedding generated")
            print(f"  Dimension: {dimension}")
            print(f"  First 5 values: {embedding[:5]}")
            return True
        else:
            print(f"✗ Single embedding failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Single embedding error: {e}")
        return False


def test_batch_embedding():
    """Test batch embedding generation."""
    print("\nTesting batch embedding...")
    try:
        texts = [
            "Reuters reports on market trends",
            "Bloomberg covers financial news",
            "Economic indicators show growth"
        ]
        response = requests.post(
            f"{EMBEDDING_SERVICE_URL}/embed/batch",
            json={"texts": texts}
        )
        if response.status_code == 200:
            data = response.json()
            embeddings = data.get("embeddings")
            dimension = data.get("dimension")
            count = data.get("count")
            print(f"✓ Batch embedding generated")
            print(f"  Count: {count}")
            print(f"  Dimension: {dimension}")
            return True
        else:
            print(f"✗ Batch embedding failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Batch embedding error: {e}")
        return False


def main():
    print("=" * 60)
    print("Embedding Service Integration Test")
    print("=" * 60)
    
    # Wait for service to be ready
    print("\nWaiting for embedding service to be ready...")
    for i in range(10):
        if test_health():
            break
        print(f"Retrying in 2 seconds... ({i+1}/10)")
        time.sleep(2)
    else:
        print("\n✗ Embedding service not available")
        return
    
    # Run tests
    results = []
    results.append(("Health Check", test_health()))
    results.append(("Single Embedding", test_single_embedding()))
    results.append(("Batch Embedding", test_batch_embedding()))
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(result[1] for result in results)
    if all_passed:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed")
    
    return all_passed


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)

