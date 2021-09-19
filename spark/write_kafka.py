sightingLoc \
  .groupBy("zip_code", window("start_time", "1 hour")) \
  .count() \
  .select( \
    to_json(struct("zip_code", "window")).alias("key"),
    col("count").cast("string").alias("value")) \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "nest-camera-stats") \
  .option("checkpointLocation", "/path/to/HDFS/dir") \
  .outputMode("complete") \
  .start()

  # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("topic", "topic1") \
    .save()