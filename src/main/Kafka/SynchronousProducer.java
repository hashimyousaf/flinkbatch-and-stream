package main.Kafka;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class SynchronousProducer {
    public static void main(String[] args) throws Exception {

        String topicName = "kafka-topic";
        String key = "Key1";
        String value = "This is Synchronous Producer";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

        // Just wrap it in try catch method
        try {
            // call get method after sending , it will wait until success or failure response comes
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset " + metadata.offset());
            System.out.println("SynchronousProducer Completed with success.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("SynchronousProducer failed with an exception");
        } finally {
            producer.close();
        }

        System.out.println("This is hashim yousaf");
    }
}
