import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

import java.io.IOException;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class TweetTextExtractorBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        Status status = (Status)tuple.getValueByField("status");
        String url = "";
        String actual_url = "";

        if (status.getURLEntities() != null && status.getURLEntities().length > 0) {
            url = status.getURLEntities()[0].getURL().trim();
            actual_url = getRedirecturl(url);
        }
        else {
            url = "Not Available";
            actual_url = "Not Available";
        }

        collector.emit(new Values(status.getUser().getScreenName(), status.getUser().getName(), status.getId(), url,
                                  actual_url, status.getText(), status.getCreatedAt()));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("screenName", "userName", "statusID", "url", "actualURl", "title",
                                                "publishedDate"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private String getRedirecturl(String url) {

        HttpURLConnection connection;
        try {
            if (url != null) {
                connection = (HttpURLConnection) new URL(url).openConnection();
                connection.connect();
                connection.setInstanceFollowRedirects(false);

                int responseCode = connection.getResponseCode();
                if((responseCode / 100) == 3) {
                    url = connection.getHeaderField("Location");
                    responseCode = connection.getResponseCode();

                    url = getRedirecturl(url);
                }
            }
        } catch(MalformedURLException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }

        return url;

    }

}
