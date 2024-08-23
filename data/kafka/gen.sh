/usr/local/src/kafka_2.13-3.7.0/bin/kafka-producer-perf-test.sh \
--topic k2es-data \
--num-records 10000000 \
--payload-delimiter "\n" \
--throughput 80000 \
--producer-props bootstrap.servers=localhost:9092  \
--print-metrics \
--payload-file /home/yudong/code/k2es/data/kafka/body


# 给topic添加消息过期配置
#/usr/local/src/kafka_2.13-3.7.0/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name k2es-data --alter --add-config retention.ms=3600000