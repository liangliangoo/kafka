package com.xiaoxiong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * @author xiongliang
 * @version 1.0
 * @description 生产者
 * @since 2022/4/4  11:31
 */
public class ProducerExample {

    private static final Properties properties;

    static {
        properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        properties.put(ACKS_CONFIG, "all");
        // 开启幂等性这个参数必须配置
        properties.put(RETRIES_CONFIG, 3);
        properties.put(BATCH_SIZE_CONFIG, 16384);
        // 是否延迟提交
        properties.put(LINGER_MS_CONFIG, 5000);
        properties.put(BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // topic-test 3 partition 3 replication
        try {
            producer.send(new ProducerRecord<>("topic-test", "hello"));
        } finally {
            producer.close();
        }
    }


}
