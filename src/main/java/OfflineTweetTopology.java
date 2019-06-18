import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class OfflineTweetTopology {

    public static String queryString = "Renzi";

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("offline-tweet-spout", new OfflineTwitterStreamSpout());

        topologyBuilder.setBolt("tweet-classif-bolt", new OfflineTweetClassifierBolt())
                        .fieldsGrouping("offline-tweet-spout", new Fields("text"));

        topologyBuilder.setBolt("tweet-writer-bolt", new OfflineTweetWriterBolt())
                        .fieldsGrouping("tweet-classif-bolt", new Fields("score"));

        Config config = new Config();
        config.setNumWorkers(1);
        final LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("tweet-realtime-sentiment", config, topologyBuilder.createTopology());

    }

}
