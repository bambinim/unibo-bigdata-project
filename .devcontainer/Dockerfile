FROM debian:bookworm AS cache

ARG SPARK_VERSION=3.5.8
WORKDIR /root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y curl && \
    curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz --output ./spark.tgz

FROM debian:bookworm

ARG SPARK_VERSION=3.5.8

WORKDIR /root

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VENV=/home/user/venv

RUN apt-get update && \
    apt-get install -y procps mandoc curl git vim less sudo zip unzip python3 python3-pip python3-dev python3-venv awscli && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=cache /root/spark.tgz ./spark.tgz
RUN tar -xzf ./spark.tgz && \
    mv ./spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark-${SPARK_VERSION} && \
    rm ./spark.tgz && \
    mkdir /opt/spark-${SPARK_VERSION}/logs && \
    printf 'spark.eventLog.enabled=true\nspark.eventLog.dir=file:///opt/spark-3.5.8/logs\nspark.history.fs.logDirectory=file:///opt/spark-3.5.8/logs\n' > /opt/spark-${SPARK_VERSION}/conf/spark-defaults.conf
RUN addgroup --system --gid 1000 user && \
    adduser --system --shell /bin/bash --home /home/user --uid 1000 user && \
    usermod -aG sudo user && \
    sed -i 's/%sudo	ALL=(ALL:ALL) ALL/%sudo	ALL=(ALL:ALL) NOPASSWD:ALL/g' /etc/sudoers && \
    chown -R user:user /opt/spark-${SPARK_VERSION}

USER user
WORKDIR /home/user
RUN python3 -m venv ${PYTHON_VENV}
ENV PATH="${PYTHON_VENV}/bin:/opt/spark-${SPARK_VERSION}/bin:${PATH}"
RUN --mount=type=bind,source=./requirements.txt,target=./requirements.txt,readonly=true \
    python -m pip install jupyter findspark spylon-kernel && \
    python -m spylon_kernel install --user && \
    python -m pip install -r ./requirements.txt

ENV HADOOP_HOME=/opt/spark-${SPARK_VERSION}
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}
ENV AWS_PROFILE=default

ENTRYPOINT [ "/bin/bash" ]
