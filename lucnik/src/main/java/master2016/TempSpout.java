package master2016;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

class TempSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private static final int window_time = 5000;

    static final String ROOM_STREAM = "roomStream";
    static final String ROOM_NAME = "roomID";
    static final String ROOM_TEMPERATURE = "roomTemperature";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    private Values randomTemperature() {
        int temperature = (int) (15 + Math.random() * 100);
        return new Values(AvailableRoom.getRandomRoom(), temperature);
    }

    public void nextTuple() {
        Values randomTemperature = this.randomTemperature();
        System.out.println("emitting " + randomTemperature);
        collector.emit(ROOM_STREAM, randomTemperature);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ROOM_STREAM, new Fields(ROOM_NAME, ROOM_TEMPERATURE));
    }
}
