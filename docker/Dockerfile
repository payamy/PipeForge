# pipeforge-job: PipeForge CLI on top of SparkLake's published spark-base image.
# PipeForge only depends on the published image tag, never on SparkLake sources.
#
#   docker build -f docker/Dockerfile \
#     --build-arg SPARK_BASE_IMAGE=registry.example.com/sparklake/spark-base \
#     --build-arg SPARK_BASE_TAG=3.5.2-delta3.2.0-1.0.0 \
#     -t pipeforge-job:2.0.0 .
ARG SPARK_BASE_IMAGE=spark-base
ARG SPARK_BASE_TAG=3.5.2-delta3.2.0
FROM ${SPARK_BASE_IMAGE}:${SPARK_BASE_TAG}

# Installed WITHOUT the [spark] extra: Spark and Delta are already in the base image.
ARG PIPEFORGE_VERSION=2.0.0
LABEL org.opencontainers.image.title="pipeforge-job" \
      org.opencontainers.image.version="${PIPEFORGE_VERSION}"

USER root
WORKDIR /opt/pipeforge-src
COPY setup.py README.md ./
COPY pipeforge ./pipeforge
RUN pip install --no-cache-dir . && rm -rf /opt/pipeforge-src \
    && python -c "import pipeforge, importlib.metadata as m; assert m.version('pipeforge') == '${PIPEFORGE_VERSION}', m.version('pipeforge')" \
    && (pip show pyspark >/dev/null 2>&1 && echo 'pyspark must not be pip-installed' && exit 1 || true)

ENV TZ=UTC \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

WORKDIR /home/spark
USER spark
ENTRYPOINT ["pipeforge"]
CMD ["run"]
