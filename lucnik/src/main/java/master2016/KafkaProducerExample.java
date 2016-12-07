package master2016;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> prod = new KafkaProducer<>(props);

        String topic = "myTopic";
        int partition = 0;
        // From the Docs:
        // The key is an optional message key that was used for partition assignment. The key can be null.
        String key = "testKey";

        boolean terminate = false;

        // TODO check whether all tweets are written to kafka
        // adding .get() at the returned object will make the method synchronous.
        // without it, the application won't wait for it before terminating
        while (!terminate) {
            String value = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
            prod.send(new ProducerRecord<>(topic, partition, key, value));
            Thread.sleep(1000);
        }
        prod.close();
    }
}
