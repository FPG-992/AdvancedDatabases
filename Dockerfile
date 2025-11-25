FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir \
        apache-sedona \
        pandas \
        matplotlib

WORKDIR /app
