package master2016;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaTweetsSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private String kafkaBrokerUrls;
    private String lang;

    public KafkaTweetsSpout(String kafkaBrokerUrls, String lang) {
        this.kafkaBrokerUrls = kafkaBrokerUrls;
        this.lang = lang;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO move hardcoded arguments to the topology
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBrokerUrls);
        // TODO set static group.id
        properties.put("group.id", ((Long) System.currentTimeMillis()).toString());
        // TODO true or false?
        properties.put("enable.auto.commit", "false");
        // properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(this.lang));
        this.collector = collector;
    }

    public void nextTuple() {
        // For polling check on the Docs.
        // https://kafka.apache.org/0100/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#poll(long)
        ConsumerRecords<String, String> records = consumer.poll(0);

        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records) {
                String hashtag = record.value();
                collector.emit(new Values(hashtag));
                System.out.println("KAFKAA " + hashtag);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
