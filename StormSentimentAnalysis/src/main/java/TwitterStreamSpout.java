import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

import twitter4j.*;
import twitter4j.auth.AccessToken;

public class TwitterStreamSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private Query query = new Query(TweetTopology.queryString);
    private Twitter twitter;
    private String last;

    /***
     * Just set some parameters of the Twitter object, in order to call the right application. The tweets language
     * can be set using the this.query.setLang() call. Since we have only two vocabularies, we recommend you to
     * choose between "it" and "en". Otherwise, the program will fetch tweets from all around the world.
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;

        twitter = new TwitterFactory().getInstance();

        twitter.setOAuthConsumer("IrC5S6W23p9WnzBNy9ouj23OM", "BC8qdmaflt6pDD5fkkqioXC6xEHpwHAaGCQmmPjgNN1ao26Y2s");
        twitter.setOAuthAccessToken(new AccessToken("821625398-d42nC6dpMQnd0fmpXAj3AQr4pbObS4uHk6rCrTpj",
                "ZCglcVVLbbOvyJ3x55Z3UIvOSiVqzGXw6kWBt0yc0jTlO"));

        this.last = " ";

        // it = italian, en = english
        this.query.setLang(TweetTopology.language);

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

    /***
     * Since the Twitter API allows a maximum of 255 calls in a 15 minutes time span, we need to slow down the
     * program: After a call to the API, it sleeps for 5 seconds. To avoid repetitions and errors, the tweet is
     * emitted to the bolts only if it hasn't been emitted previously.
     */
    @Override
    public void nextTuple() {

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
