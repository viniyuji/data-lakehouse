FROM flink:1.20.5-scala_2.12

# Enable built-in Presto S3 filesystem plugin (recommended for S3/MinIO checkpoints)
RUN mkdir -p /opt/flink/plugins/s3-fs-presto && \
    cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/

# Add Flink SQL Kafka connector (supports 'kafka' and 'upsert-kafka')
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

RUN chown -R flink:flink /opt/flink/lib /opt/flink/plugins
