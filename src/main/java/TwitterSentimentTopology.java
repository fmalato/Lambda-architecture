import java.util.*;
import java.util.logging.Logger;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.LogWriter;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

/**
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <topic-name> <keyword>
 * <comsumerKey>	- Twitter consumer key
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>	- Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * <keyword>		- The keyword to filter tweets
 *
 * More discussion at stdatalabs.blogspot.com
 *
 * @author Sachin Thirumala
 */

public class TwitterSentimentTopology {

    public static final String HBASE_CONFIG_KEY = "hbase.conf";
    public static final Log log = LogFactory.getLog(TwitterSentimentTopology.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // TopologyBuilder instance. Defines the data flow between the components in the topology.
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // Register the twitter spout and assign it a unique id.
        topologyBuilder.setSpout("twitter-stream-spout", new TwitterStreamSpout());
        /*
         *  TwitterSpout ----> DetailsExtractorBolt
         *  Assigned parallelism 2 for DetailsExtractorBolt.
         */
        log.info("Spout superato");
        topologyBuilder.setBolt("tweet-text-extractor-bolt", new TweetTextExtractorBolt(), 2)
                .shuffleGrouping("twitter-stream-spout");
        /*
         *  TwitterSpout ----> RetweetDetailsExtractorBolt
         *  Assigned parallelism 2 for RetweetDetailsExtractorBolt.
         */
        log.info("Tweet-text inizializzato");
        topologyBuilder.setBolt("retweet-text-extractor-bolt", new RetweetTextExtractorBolt(), 2)
                .shuffleGrouping("twitter-stream-spout");
        // Join DetailsExtractorBolt and RetweetDetailsExtractorBolt ----> FileWriterBolt
        /*SimpleHBaseMapper mapper = new SimpleHBaseMapper().withColumnFields(new Fields("screenName",
                                                                "userName", "statusID", "url", "actualURl", "title",
                                                                "publishedDate"));
        topologyBuilder.setBolt("HBase-writer-bolt", (IRichBolt) new HBaseBolt("tweets", mapper))
                .shuffleGrouping("twitter-stream-spout")
                .shuffleGrouping("retweet-text-extractor-bolt");*/
        // Config instance. It defines topology's runtime behavior.
        log.info("Retweet-text inizializzato");
        topologyBuilder.setBolt("file-writer-bolt", new FileWriterBolt())
                .fieldsGrouping("tweet-text-extractor-bolt", new Fields("title"))
                .fieldsGrouping("retweet-text-extractor-bolt", new Fields("title"));
        log.info("File writer inizializzato");
        Config config = new Config();
        /*Map<String, String> HBConfig = Maps.newHashMap();
        HBConfig.put("hbase.rootdir", "file:/home/federico/hadoop/hbase-1.4.9");
        HBConfig.put("hbase.zookeeper.property.dataDir", "/home/federico/hadoop/zookeeper-3.4.14");
        HBConfig.put("hbase.cluster.distributed", "true");
        config.put("HBCONFIG", HBConfig);*/
        // To run storm in local mode.

        if (args != null && args.length > 0) {
            config.setNumWorkers(1);
            log.info("Worker inizializzato");
            try {
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
                log.info("Topology accettata");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            final LocalCluster cluster = new LocalCluster();
            // deploy topology in local mode.
            cluster.submitTopology("twitter-realtime-sentiment-topology", config, topologyBuilder.createTopology());
            // This method will kill the topology while shutdown the JVM.
            log.info("Topology accepted");
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    cluster.killTopology("twitter-realtime-sentiment-topology");
                    cluster.shutdown();
                    log.info("Topology distrutta");
                }
            });
        }
    }

}
