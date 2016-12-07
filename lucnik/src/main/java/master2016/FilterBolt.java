package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

class FilterBolt extends BaseRichBolt {

    private String fieldName;
    private int thr;
    private String alarmLabel;

    FilterBolt(String fieldName, int thr, String alarmLabel) {
        this.fieldName = fieldName;
        this.thr = thr;
        this.alarmLabel = alarmLabel;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    public void execute(Tuple input) {
        int roomID = (Integer) input.getValueByField(TempSpout.ROOM_NAME);
        int valueByField = (Integer) input.getValueByField(this.fieldName);

        if (valueByField > thr) {
            System.out.println(this.alarmLabel + ", " + roomID + ", " + valueByField);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output stream
    }
}
