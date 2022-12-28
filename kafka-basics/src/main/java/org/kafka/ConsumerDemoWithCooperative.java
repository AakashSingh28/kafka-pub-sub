package org.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithCooperative {

    private static final Logger log= LoggerFactory.getLogger(ConsumerDemoWithCooperative.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am Kafka Consumer");
        String bootstrapServers="127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_java";

        Properties  properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"none/earliest/latest");

        //create Consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);

        // get a  reference to the current Thread
        final Thread mainThread=Thread.currentThread();

        // adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                log.info("Detected a shutdown , lets's exit by calling consumer.wakeup()....");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                //join the main thread to allow the execution of the code in the main thread
            }
        });


        try{
        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singletonList((topic)));

        while (true){
            log.info("Polling");
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                log.info("key: " + record.key() + "value: " + record.value());
                log.info("Partition: " + record.key() + "Offset:: " + record.value());
            });
        }}catch (WakeupException e) {
          log.info(" Wake up exception ");
        } catch (Exception e) {
            log.error(" Unexpected Exception ");
        } finally {
          consumer.close(); // this will also commit the offsets if need to be
          log.info("The Consumer is gratefully closed ");
        }
    }
}
