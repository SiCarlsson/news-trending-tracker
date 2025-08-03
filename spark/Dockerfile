FROM ubuntu:22.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update system packages and install required dependencies
RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y --no-install-recommends \
  openjdk-11-jdk \
  python3 \
  python3-pip \
  ca-certificates \
  procps \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


# Set Java environment variable (required for PySpark)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Python optimizations
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Create non-root user for security
RUN groupadd -r -g 1001 spark && \
  useradd -r -g spark -u 1001 -m spark

WORKDIR /app
USER spark

# Install Python dependencies
COPY requirements.txt .
RUN python3 -m pip install --user --no-cache-dir --upgrade pip && \
  python3 -m pip install --user --no-cache-dir -r requirements.txt

# Add user's local bin to PATH so pip-installed executables can be found
ENV PATH=/home/spark/.local/bin:${PATH}

# Copy application code with proper ownership
COPY --chown=spark:spark . .

# Add a health check to ensure the app stays running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep -f "python.*main.py" > /dev/null || exit 1

# Default command to run the application
CMD ["python3", "main.py"]