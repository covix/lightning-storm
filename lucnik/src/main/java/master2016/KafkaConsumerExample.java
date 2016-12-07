package master2016;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put("group.id", "myGroup");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList("master2016-replicated-java", "myTopic"));
            while (true) {
                // For polling check on the Docs.
                // https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll(long)
                ConsumerRecords<String, String> records = consumer.poll(10);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.print("Topic: " + record.topic() + ", ");
                    System.out.print("Partition: " + record.partition() + ", ");
                    System.out.print("Key: " + record.key() + ", ");
                    System.out.println("Value: " + record.value() + ", ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

