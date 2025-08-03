FROM bitnami/spark:3.5.3

# Set working directory
WORKDIR /app

# Set up the Python path to include Spark's Python libraries
ENV PYTHONPATH=/app:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_HOME=/opt/bitnami/spark

# Copy requirements and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Add a health check to ensure the app stays running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD ps aux | grep -v grep | grep -q python || exit 1

# Default command
CMD [ "python", "main.py" ]