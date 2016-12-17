package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class HashtagReaderBolt extends BaseRichBolt {
    private String keyword;
    private OutputCollector collector;
    private boolean windowOpen;

    public HashtagReaderBolt(String keyword) {
        this.keyword = keyword;
        this.windowOpen = false;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String hashtag = tuple.getStringByField("hashtag");
        if (!this.windowOpen) {
            if (this.keyword.equals(hashtag)) {
                this.windowOpen = true;
                this.collector.emit(new Values(hashtag));
            }
        } else {
            this.collector.emit(new Values(hashtag));
        }

        // if (this.keyword.equals(hashtag)) {
        //     this.windowOpen = true;
        // }
        //
        // if (windowOpen) {
        //     this.collector.emit(new Values(hashtag));
        // }

        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
