package master2016;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class HashtagCounterBolt extends BaseRichBolt {
    private String keyword;
    private boolean windowOpen;
    private Object2IntOpenHashMap<String> counterMap;
    private OutputCollector collector;

    public HashtagCounterBolt(String keyword) {
        this.keyword = keyword;
        this.windowOpen = false;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counterMap = new Object2IntOpenHashMap<>();
    }

    public void execute(Tuple tuple) {
        String hashtag = tuple.getStringByField("hashtag");

        if (this.keyword.equals(hashtag)) {
            if (!this.windowOpen) {  // if the window was closed
                this.windowOpen = true;
            } else {
                this.collector.emit(new Values(this.counterMap));
                this.counterMap = new Object2IntOpenHashMap<>();
            }
        } else {
            if (!this.counterMap.containsKey(hashtag)) {
                this.counterMap.put(hashtag, 1);
            } else {
                this.counterMap.put(hashtag, this.counterMap.getInt(hashtag) + 1);
            }
        }

        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("map"));
    }
}
