FROM apache/spark:3.5.3-python3

USER root
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app
RUN ln -sf /usr/bin/python3 /usr/bin/python

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Попереднє завантаження JAR-драйверів PostgreSQL та Kafka у /opt/spark/jars
RUN python3 -c "open('/tmp/_noop.py', 'w').write('pass\n')" \
    && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4 /tmp/_noop.py \
    && cp /root/.ivy2/jars/*.jar /opt/spark/jars/ \
    && rm -rf /root/.ivy2 /tmp/_noop.py

COPY . /app

ENTRYPOINT []
