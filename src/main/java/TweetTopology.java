import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class TweetTopology {

    public final static String queryString = "computer";

    /*** A simple initialization of the complete topology. The static variable queryString is used from every
     * spout and bolt.
     */

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("tweet-spout", new TwitterStreamSpout());

        topologyBuilder.setBolt("tweet-classif-bolt", new TweetClassifierBolt())
                        .fieldsGrouping("tweet-spout", new Fields("text"));

        topologyBuilder.setBolt("tweet-writer-bolt", new TweetWriterBolt())
                        .fieldsGrouping("tweet-classif-bolt", new Fields("score"));

        Config config = new Config();
        config.setNumWorkers(1);
        final LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("tweet-realtime-sentiment", config, topologyBuilder.createTopology());

    }

}
