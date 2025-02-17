# Use the official Apache Flink image
FROM flink:1.14.4-scala_2.12

# Install Python and other dependencies
RUN apt-get update && apt-get install -y python3 python3-pip

# Install Jupyter Notebook
RUN pip3 install jupyter

# Install PyFlink
RUN pip3 install apache-flink

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]