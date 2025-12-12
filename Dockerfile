FROM apache/spark:3.5.0

USER root

# Install dependencies and Python packages
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir \
        apache-sedona \
        pandas \
        matplotlib

# Download Hadoop for client libraries (needed for HDFS access)
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# Configure Spark to use Hadoop
ENV SPARK_DIST_CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*"

# Copy Hadoop config for HDFS access
COPY hadoop-config/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY hadoop-config/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

WORKDIR /app
