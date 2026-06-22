FROM flink:1.18.1-java17
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL -o /opt/flink/lib/flink-metrics-prometheus-1.18.1.jar \
       https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER flink
