/usr/local/src/kafka_2.13-3.7.0/bin/kafka-producer-perf-test.sh \
--topic k2es-data \
--num-records 100 \
--throughput -20 \
--producer-props bootstrap.servers=localhost:9092  \
--payload-file ./body