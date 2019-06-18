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
    private ArrayList<String> lineBuffer;
    private Iterator lineBufferItr;
    private Query query = new Query("ciao");
    private Twitter twitter;
    private ArrayList<String> tweets;
    private Iterator itr;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;

        twitter = new TwitterFactory().getInstance();

        twitter.setOAuthConsumer("IrC5S6W23p9WnzBNy9ouj23OM", "BC8qdmaflt6pDD5fkkqioXC6xEHpwHAaGCQmmPjgNN1ao26Y2s");
        twitter.setOAuthAccessToken(new AccessToken("821625398-d42nC6dpMQnd0fmpXAj3AQr4pbObS4uHk6rCrTpj",
                "ZCglcVVLbbOvyJ3x55Z3UIvOSiVqzGXw6kWBt0yc0jTlO"));

        this.tweets = new ArrayList<String>();
        this.itr = tweets.iterator();

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
        Utils.sleep(millis);

        try {
            QueryResult result;
            result = twitter.search(query);

            for (Status tweet : result.getTweets()) {
                this.tweets.add(tweet.getText());
                System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                collector.emit(new Values(itr.next()));
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
