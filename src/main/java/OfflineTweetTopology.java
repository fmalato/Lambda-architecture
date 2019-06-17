import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class OfflineTweetTopology {

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("offline-tweet-spout", new OfflineTwitterStreamSpout());

        topologyBuilder.setBolt("text-extractor-bolt", new OfflineTweetTextExtractorBolt())
                        .fieldsGrouping("offline-tweet-spout", new Fields("tweet-id", "date", "text"));

        topologyBuilder.setBolt("tweet-classif-bolt", new OfflineTweetClassifierBolt())
                        .fieldsGrouping("text-extractor-bolt", new Fields("tweet-id", "date", "words"));

        topologyBuilder.setBolt("tweet-writer-bolt", new OfflineTweetWriterBolt())
                        .fieldsGrouping("tweet-classif-bolt", new Fields("tweet-id", "date", "text", "score"));

        Config config = new Config();
        final LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("tweet-realtime-sentiment", config, topologyBuilder.createTopology());

    }

}
