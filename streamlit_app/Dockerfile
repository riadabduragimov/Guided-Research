# Use Debian slim as base (manual installation of Java)
FROM debian:bullseye-slim

# Install required packages: wget, tar, python3, pip, bash
RUN apt-get update && \
    apt-get install -y wget tar python3 python3-pip bash && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download and extract OpenJDK 8 manually 
RUN wget https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u362-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u362b09.tar.gz -O /tmp/jdk8.tar.gz && \
    tar -xzf /tmp/jdk8.tar.gz -C /opt && \
    rm /tmp/jdk8.tar.gz

# Set Java environment variables
ENV JAVA_HOME=/opt/jdk8u362-b09
ENV PATH="$JAVA_HOME/bin:$PATH"

# Upgrade pip and install Python dependencies 
RUN pip3 install --upgrade pip && \
    pip3 install streamlit pyspark==3.0.0 pyhive pymysql psycopg2-binary plotly

# Create Spark config directory
RUN mkdir -p /opt/spark/conf

# Copy hive-site.xml to Spark config
COPY hive-site.xml /opt/spark/conf/hive-site.xml

# Copy app code into /app
WORKDIR /app
COPY . /app

# Make sure spark-env.sh sets JAVA_HOME 
RUN echo 'export JAVA_HOME=/opt/jdk8u362-b09' >> /opt/spark/conf/spark-env.sh

# Expose streamlit port
EXPOSE 8501

# Run streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
