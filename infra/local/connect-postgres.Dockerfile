FROM quay.io/debezium/connect:2.7

RUN set -eux; \
    find /kafka/connect -mindepth 1 -maxdepth 1 -type d ! -name debezium-connector-postgres -exec rm -rf {} +; \
    find /kafka/connect -mindepth 1 -maxdepth 1 -type d -print
