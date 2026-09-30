FROM flink:1.18.1-java17
USER root
# The pinned Flink image already includes curl and its matching Prometheus JAR.
# Reuse that artifact instead of downloading OS indexes and the same JAR again.
RUN cp /opt/flink/plugins/metrics-prometheus/flink-metrics-prometheus-1.18.1.jar /opt/flink/lib/
USER flink
COPY --chown=flink:flink build/docker/fluxmeter.jar /opt/flink/usrlib/fluxmeter.jar

COPY --chown=flink:flink config/pricing.json /opt/flink/usrlib/pricing.json
ENV PRICING_FILE=/opt/flink/usrlib/pricing.json
