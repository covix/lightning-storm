package twitter.nik;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.User;

import java.util.Map;



public class StringWordSplitterBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5151173513759399636L;

	private final int minWordLength;

    private OutputCollector collector;

    public StringWordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Status tweet = (Status) input.getValueByField("tweet");
        String lang = tweet.getUser().getLang();
        String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
    	//String tweet = (String) input.getValueByField("tweet");
    	//String lang = "en";
    	//String text = tweet.replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minWordLength) {
                //collector.emit(new Values(lang, word));
                collector.emit(input, new Values(lang, word));
            }
        }
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
