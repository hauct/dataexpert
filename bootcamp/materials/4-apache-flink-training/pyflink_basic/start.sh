#!/bin/bash

# Khởi động Flink cluster ở chế độ nền
/docker-entrypoint.sh jobmanager &

# Khởi động Jupyter Lab ở chế độ foreground
exec jupyter lab --ip=0.0.0.0 --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token='' \
    --notebook-dir=/opt/flink/notebooks