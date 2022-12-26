package org.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log= LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am Kafka Producer");

        // create Producer Properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create Producer

        KafkaProducer<String,String> producer=new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world "+i);


            //send data - asynchronous operation
            producer.send(producerRecord, (metadata, exception) -> {
                // Execute everytime a record is successfully sent or an exception is thrown
                if (exception == null) {
                    log.info("Received the metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n"
                    );
                } else {
                    log.error("Error while producing", exception);
                }

               /* try{
                    Thread.sleep(1000);
                }catch (Exception e){
                    log.error("Exception ",e);
                }*/
            });
        }
        //flush and close the Producer
        producer.flush();   // It will block the process until producer.send gets complete

        // flush and close produce
        producer.close();

    }
}
