FROM apache/airflow:2.9.0

USER root

# Install Java (required by PySpark in the processing notebooks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17-openjdk

# Stable path for amd64 and arm64 (Apple Silicon); PySpark uses JAVA_HOME to launch the JVM.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

USER airflow

# Install Python dependencies needed by DAGs and notebooks
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt