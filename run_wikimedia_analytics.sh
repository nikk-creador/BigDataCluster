#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
  --conf spark.jars.ivy=/tmp/.ivy2.5.2 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf "spark.driver.host=${SPARK_CLUSTER_PUBLIC_HOST:-spark-master}" \
  --conf spark.driver.port=7080 \
  --conf spark.driver.blockManager.port=7081 \
  --conf spark.executor.port=7082 \
  --conf spark.blockManager.port=7083 \
  --conf spark.executor.cores=2 \
  --conf "spark.cores.max=${SPARK_MAX_CORES:-12}" \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  /opt/spark-apps/spark_wikimedia_analytics.py
