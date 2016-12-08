package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

class DebugBolt extends BaseRichBolt {
    private OutputCollector collector;
    private int count;
    private Fields fields;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.count = 0;
    }

    public void execute(Tuple tuple) {
        if (this.fields == null)
            this.fields = tuple.getFields();

        this.count += 1;

        String tupleString = tuple.getString(0);
        // System.out.println("[DEBUG] new tuple received");
        for (String fieldName : tuple.getFields()) {
            // System.out.println(fieldName + ": " + tuple.getValueByField(fieldName));
        }
        // System.out.println();

        // TODO no sé if it is necesseray to do it
        collector.ack(tuple);

        this.collector.emit(new Values(tuple));
    }

    public void cleanup() {
        System.out.println("[DEBUG] counted tuples: " + this.count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
