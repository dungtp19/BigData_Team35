
1. Chạy Docker

cd \sentiment-pipeline

docker compose up -d

docker ps

2. Vào container Spark
docker exec -it spark bash
cd /opt/spark/work-dir

--- Init Ivy cache 
mkdir -p /opt/spark/work-dir/.ivy2/cache

--- Kiểm tra PostgreSQL JDBC jar
cp /opt/spark/work-dir/jars/postgresql-42.7.3.jar /opt/spark/jars/
ls -l /opt/spark/jars/postgresql-42.7.3.jar

--- Export biến môi trường
export PYTHONPATH=/opt/spark/work-dir/python-libs
export NLTK_DATA=/opt/spark/work-dir/python-libs/nltk_data
export SPARK_SUBMIT_OPTS="-Divy.home=/opt/spark/work-dir/.ivy2"

--- Spark sumit
/opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.driver.extraClassPath=/opt/spark/jars/postgresql-42.7.3.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 \
  sentiment_stream.py

3. Vào container Kafka
docker exec -it kafka bash

--- Kiểm tra topic tồn tại chưa
kafka-topics --bootstrap-server localhost:9092 --list

--- Chạy Kafka producer
kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic social_raw

--- Chạy Kafka consumer
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic social_raw \
  --from-beginning

4. Vào container Postgres
docker exec -it postgres psql -U admin -d sentiment

--- Query kiểm tra dữ liệu
SELECT * 
FROM sentiment_results
ORDER BY id DESC
LIMIT 10;

SELECT * 
FROM sentiment_results
ORDER BY id
LIMIT 10;

SELECT COUNT(*) FROM sentiment_results;

4. Vào Grafana xem dashboard realtime
http://localhost:3000/

5. Chạy X producer
cd \sentiment-pipeline\producer

--- kiểm tra version Python > 3.9
python --version 

--- Activate venv
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1

--- Cài thư viện
pip install requests kafka-python

--- Chạy X Producer
python x_producer.py
