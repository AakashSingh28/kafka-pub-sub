package org.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log= LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am Kafka Consumer");
        String bootstrapServers="127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo_java";

        Properties  properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"none/earliest/latest");

        //create Consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singletonList((topic)));

        while (true){
            log.info("Polling");
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                log.info("key: " + record.key() + "value: " + record.value());
                log.info("Partition: " + record.key() + "Offset:: " + record.value());
            });
        }
    }
}
