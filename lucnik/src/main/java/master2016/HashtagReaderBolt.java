package master2016;

import java.util.Map;

import twitter4j.Status;
import twitter4j.HashtagEntity;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagReaderBolt extends BaseRichBolt {
	private OutputCollector collector;
	String keyWord;
	Boolean start = false;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		keyWord = (String) conf.get("my.keyWord");
	}

	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValueByField("tweet");
		String lang = tweet.getLang();

		for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
			// TODO shall we emit lowercase hashtags? YES! (at least for the internal comparison?
			// System.out.println("Hashtag: " + hashtag.getText());
			System.out.println(hashtag.getText());
			System.out.println("#### TEST "+keyWord+ " "+ hashtag.getText());			
			if (keyWord.equals((String)hashtag.getText().toLowerCase())) {
				start = !start;
				System.out.println("WINDOWSTART/END");
				// TODO skip the keyword-hashtag for emitting!
			}
		
			if (start == true) {
				this.collector.emit(new Values(lang, hashtag.getText().toLowerCase()));
				System.out.println("EMIT: "+hashtag.getText());
			}

		}
		System.out.println("CHACK");

	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("lang", "hashtag"));
	}
}
