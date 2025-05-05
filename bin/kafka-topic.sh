#!/bin/bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic movie-analytics || echo "Topic already exists bruh"