package master2016;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.awt.*;
import java.awt.image.ImagingOpException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaTweetsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private String kafkaBrokerUrls;

    public KafkaTweetsSpout(String kafkaBrokerUrls) {
        this.kafkaBrokerUrls = kafkaBrokerUrls;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.println("[KAFKA] opening method called");

        // TODO move hardcoded arguments to the topology
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBrokerUrls);
        // TODO set static group.id
        properties.put("group.id", ((Long) System.currentTimeMillis()).toString());
        // TODO true or false?
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // This could not be thread safe
        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("twitter"));
        this.collector = collector;
    }

    public void nextTuple() {
        // System.out.println("[KAFKA] Someone asked for tuple!");
        // For polling check on the Docs.
        // https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll(long)
        ConsumerRecords<String, String> records = consumer.poll(0);

        if (!records.isEmpty()) {
            int count = 0;
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Status status = TwitterObjectFactory.createStatus(record.value());
                    String lang = status.getLang();
                    // non blocking operation
                    collector.emit(new Values(lang, status));

                    // System.out.println("[KAFKA] emitted");
                    count += 1;

                } catch (TwitterException e) {
                    // e.printStackTrace();
                }
            }
            // System.out.println("[KAFKA] window of: " + count );
            // System.out.println("[KAFKA] avg: " + avg);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "tweet"));
    }
}
