FROM python:3.11-slim-bookworm

USER root

# Install Java required by PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/retailstream

# Install Python dependencies
COPY requirements.txt /opt/retailstream/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Project scripts
COPY scripts /opt/retailstream/scripts

# Project directories used by the batch pipeline
RUN mkdir -p \
    /opt/retailstream/data/bronze \
    /opt/retailstream/data/silver \
    /opt/retailstream/data/.raw_from_rustfs

CMD ["python", "--version"]