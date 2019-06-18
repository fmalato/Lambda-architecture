import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class OfflineTwitterStreamSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private Query query = new Query("Trump");
    private Twitter twitter;
    private String last;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;

        twitter = new TwitterFactory().getInstance();

        twitter.setOAuthConsumer("JBAbc7FNeim1R3HjoagrTJTcO", "V61L1iZX5XjLWPB5hP1R5aN8wrIag6i6Ko4PQLUm0yk5a5a12q");
        twitter.setOAuthAccessToken(new AccessToken("1121726044229328896-Bece1mGPP4osyYqxLEXVxjfGuEt4rb",
                "s5sjsYWHVoIukjfdGPYsSP270w7160BO6If9YrE7DbO50"));

        this.last = " ";

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

        Random random = new Random();
        int millis = random.nextInt(30) + 10;
        Utils.sleep(5000);

        try {
            QueryResult result;
            result = twitter.search(query);
            List<Status> tweets = result.getTweets();

            if(!last.equals(tweets.get(0).getText())) {
                last = tweets.get(0).getText();
                System.out.println("NEW: " + tweets.get(0).getText());
                collector.emit(new Values(tweets.get(0).getText()));
            }
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
