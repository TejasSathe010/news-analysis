# Dockerfile for Stream Processing service

# Use a base image with Apache Spark installed
FROM apache/spark:latest

# Set the working directory
WORKDIR /app

# Copy the application files into the container
COPY . /app

# Specify any additional dependencies or setup steps
# For example, you might install additional Python packages or configure Spark settings here

# Command to run the Stream Processing application
CMD ["spark-submit", "your_stream_processing_script.py"]
