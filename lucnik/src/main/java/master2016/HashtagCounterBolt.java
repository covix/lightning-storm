package master2016;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import twitter4j.LanguageJSONImpl;

public class HashtagCounterBolt extends BaseRichBolt {
	private HashMap<String, HashMap<String, Integer>> langCounterMap;
	private HashMap<String, HashMap<String, Integer>> langCounterMap2;
	HashMap<String, Integer> counterMap;
	HashMap<String, Integer> counterMap2;
	private OutputCollector collector;
	private static final int N_RESULT = 3;
	private static final String GROUP_ID = "03";
	String keyWord;
	int window = 0;
	boolean tracker = false;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.langCounterMap = new HashMap<>();
		this.langCounterMap2 = new HashMap<>();
		this.collector = collector;
		keyWord = (String) conf.get("my.keyWord");
	}

	public void execute(Tuple tuple) {
		String lang = tuple.getStringByField("lang");

		if (keyWord.equals(tuple.getStringByField("hashtag"))) {
			System.out.println("ALAMR" + keyWord + tuple.getStringByField("hashtag"));
			window++;
		}
		if (window == 1) {
			if (!langCounterMap.containsKey(lang)) {
				langCounterMap.put(lang, new HashMap<String, Integer>());
			} else {
				counterMap = langCounterMap.get(lang);
				String hashtag = tuple.getStringByField("hashtag");

				if (!counterMap.containsKey(hashtag)) {
					counterMap.put(hashtag, 1);
				} else {
					Integer c = counterMap.get(hashtag) + 1;
					counterMap.put(hashtag, c);
				}

			}
		}

		if (window == 2) {
			System.out.println("DOOOOS");
			if (!langCounterMap2.containsKey(lang)) {
				langCounterMap2.put(lang, new HashMap<String, Integer>());
			} else {
				counterMap2 = langCounterMap2.get(lang);
				String hashtag2 = tuple.getStringByField("hashtag");

				if (!counterMap2.containsKey(hashtag2)) {
					counterMap2.put(hashtag2, 1);
				} else {
					Integer c = counterMap2.get(hashtag2) + 1;
					counterMap2.put(hashtag2, c);
				}
			}

		}

		if (window == 3) {
			System.out.println("FERTIG");
			window = 2;
			counterMap = counterMap2;
			langCounterMap=langCounterMap2;
			counterMap2.clear();
			langCounterMap2.clear();

		}

		collector.ack(tuple);
	}

	public void cleanup() {
		System.out.println("output" + keyWord);
		int windowNumber = -1;

		for (Map.Entry<String, HashMap<String, Integer>> entry : langCounterMap.entrySet()) {
			Map<String, Integer> counterMap = entry.getValue();
			String lang = entry.getKey();

			String[] hashtags = new String[N_RESULT];
			int[] counts = new int[N_RESULT];

			// instead of ordering O(nlogn) simply look for the 3 most present
			// hashtags each time
			// TODO in case of tie wins the alphabetical order => order the
			// map..
			for (int i = 0; i < N_RESULT; i++) {
				String hashtag = "null";
				int count = 0;

				for (Map.Entry<String, Integer> hashtagCount : counterMap.entrySet()) {
					if (hashtagCount.getValue() > count) {
						hashtag = hashtagCount.getKey();
						count = hashtagCount.getValue();
					}
				}
				hashtags[i] = hashtag;
				counts[i] = count;
				counterMap.remove(hashtag);
			}

			String r = "";
			for (int i = 0; i < hashtags.length; i++) {
				r += hashtags[i] + "," + counts[i] + ",";
			}
			r = r.substring(0, r.length() - 1);

			// TODO what if tweets for a given language are not found? should we
			// create the file anyway?
			// TODO shall we initialize langCounterMap with the list of
			// languages?
			System.out.println(windowNumber + "," + lang + "," + r);
			try {
				PrintWriter writer = new PrintWriter(lang + "_" + GROUP_ID + ".log", "UTF-8");
				writer.println(windowNumber + "," + lang + "," + r);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}
}
