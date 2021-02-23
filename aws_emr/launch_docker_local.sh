#!/bin/sh

# This should work but an image for the new Apple Silicon architecture does not exist yet so can't test.

docker run \
  --rm \
  --name=jobmanager \
  --network flink-network \
  --publish 8081:8081 \
  flink:1.12.0-scala_2.11 jobmanager

exit 0
