# lifo-spark: Spark 4.0.5 with Delta Lake, S3A and the lifo_spark jobs, for the Spark Operator.
#   docker build -t registry.example.com/lifo-spark:0.1.0 .
FROM apache/spark:4.0.5-scala2.13-java21-python3-ubuntu

USER root

# Delta Lake 4.0.1 (built for Spark 4.0), and S3A for Spark's Hadoop 3.4.1 with the AWS SDK v2
# bundle Hadoop 3.4.1 was built against. Baked in, so a job needs no network to start.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -ex; cd /opt/spark/jars; \
    wget -nv $MAVEN/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar; \
    wget -nv $MAVEN/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar; \
    wget -nv $MAVEN/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar; \
    wget -nv $MAVEN/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar; \
    chown spark:spark delta-*.jar hadoop-aws-*.jar bundle-*.jar

# The sweep kernel runs in every executor's Python workers. These are the versions the
# local check passed with, on the image's Python 3.10.
RUN pip install --no-cache-dir numpy==2.2.6 numba==0.68.1 pandas==2.3.3 pyarrow==25.0.1

# The jobs, importable by the driver and every Python worker
COPY jobs /opt/lifo/jobs
RUN echo /opt/lifo/jobs > "$(python3 -c 'import site; print(site.getsitepackages()[0])')/lifo_spark.pth"
ENV NUMBA_CACHE_DIR=/tmp/numba-cache

USER spark
