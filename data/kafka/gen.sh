#/usr/local/src/kafka_2.13-3.7.0/bin/kafka-producer-perf-test.sh \
/usr/local/kafka_2.13-3.8.0/bin/kafka-producer-perf-test.sh \
--topic k2es-data \
--num-records 10000000 \
--throughput -1 \
--producer-props bootstrap.servers=localhost:9092  \
--payload-file ./body

#--payload-delimiter "\n" \


# 给topic添加消息过期配置
#/usr/local/kafka_2.13-3.8.0/bin/kafka-configs.sh \
#--bootstrap-server localhost:9092 \
#--entity-type topics --entity-name k2es-data --alter --add-config retention.ms=3600000