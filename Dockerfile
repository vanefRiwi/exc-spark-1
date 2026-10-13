FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root
WORKDIR /opt/spark-app

COPY src/sales_analysis.py /opt/spark-app/src/sales_analysis.py

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--master", "local[*]", "/opt/spark-app/src/sales_analysis.py"]
