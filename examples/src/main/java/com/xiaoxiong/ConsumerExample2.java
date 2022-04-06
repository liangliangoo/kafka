package com.xiaoxiong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * @author xiongliang
 * @version 1.0
 * @description 订阅者
 * @since 2022/4/5  12:45
 */
public class ConsumerExample2 {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        properties.put(GROUP_ID_CONFIG, "G2");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        while (true) {
            kafkaConsumer.subscribe(Collections.singletonList("topic-test"));
            long startTime = System.currentTimeMillis();
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, value = %s, topic = %s", record.offset(), record.value(), record.topic());
                    System.out.println("=====================>");
                }
                long endTime = System.currentTimeMillis();
                if (endTime - startTime > 30000) {
                    System.out.println("------------------------------------------------------------------");
                    break;
                }
            }
        }
    }

}
