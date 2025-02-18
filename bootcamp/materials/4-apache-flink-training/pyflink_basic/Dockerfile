FROM apache/flink:1.18.0-scala_2.12-java11-python3.10

# Cài đặt các phụ thuộc
RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip install jupyterlab

# Cổng cho Flink UI và Jupyter
EXPOSE 8081 8888

# Copy script khởi động
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Thư mục làm việc
WORKDIR /opt/flink/notebooks

ENTRYPOINT ["/start.sh"]