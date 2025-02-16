# Sử dụng image gốc
FROM tabulario/spark-iceberg

# Cài đặt các thư viện cần thiết
RUN apt-get update && apt-get install -y wget

# Tải và thêm thư viện hadoop-aws và aws-java-sdk
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P $SPARK_HOME/jars/
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.569/aws-java-sdk-bundle-1.12.569.jar -P $SPARK_HOME/jars/

# Xóa cache
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Thêm cấu hình S3A vào spark-defaults.conf
RUN echo "spark.hadoop.fs.s3a.access.key=admin" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.secret.key=password" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.endpoint=http://minio:9000" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.path.style.access=true" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_HOME/conf/spark-defaults.conf