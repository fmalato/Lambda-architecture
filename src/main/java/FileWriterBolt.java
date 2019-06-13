import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * This bolt join the tuples from both DetailsExtractorBolt and RetweetDetailsExtractorBolt.
 * This write all the details in a csv file.
 */
public class FileWriterBolt implements IRichBolt {
    // serial version id.
    private static final long serialVersionUID = 1L;
    // File object.
    private File file = null;
    // FileWriter object.
    private FileWriter fileWriter = null;

    @Override
    public void cleanup() {

    }

    /* (non-Javadoc)
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     * This method process the details from both DetailsExtractorBolt and RetweetDetailsExtractorBolt.
     */
    @Override
    public void execute(Tuple tuple) {
        String screenName = "";
        String userName = "";
        Long statusID = null;
        String url = "";
        String actualURL = "";
        Date publishedDate = null;
        boolean isRetweet = false;
        String retweetScreenName = "";
        String retweetUserName = "";
        String title = "";
//        checks whether coming tuple is from DetailsExtractorBolt or RetweetDetailsExtractorBolt.
        if ("retweetDetailsExtractorBolt".equals(tuple.getSourceComponent())) {
            title = (String) tuple.getValueByField("title");
            isRetweet = (Boolean) tuple.getValueByField("isRetweet");
            retweetScreenName = (String) tuple.getValueByField("retweetScreenName");
            retweetUserName = (String) tuple.getValueByField("retweetUserName");
            FileDetails fileDetails = TweetDetailsManager.get(title);

            if (fileDetails == null) {
                fileDetails = new FileDetails();
            }
            fileDetails.setIsRetweet(String.valueOf(isRetweet));
            fileDetails.setRetweetScreenName(retweetScreenName);
            fileDetails.setRetweetUserName(retweetUserName);
            fileDetails.isRetweetBoltExec(true);
            TweetDetailsManager.addDetails(title, fileDetails);
        } else if ("detailsExtractorBolt".equals(tuple.getSourceComponent())) {
            screenName = (String) tuple.getValueByField("screenName");
            userName = (String) tuple.getValueByField("userName");
            statusID = (Long) tuple.getValueByField("statusID");
            url = (String) tuple.getValueByField("url");
            actualURL = (String) tuple.getValueByField("actualURl");
            title = (String) tuple.getValueByField("title");
            publishedDate = (Date) tuple.getValueByField("publishedDate");
            FileDetails fileDetails = TweetDetailsManager.get(title);

            if (fileDetails == null) {
                fileDetails = new FileDetails();
            }
            fileDetails.setScreenName(screenName);
            fileDetails.setUserName(userName);
            fileDetails.setStatusID(String.valueOf(statusID));
            fileDetails.setUrl(url);
            fileDetails.setActualURL(actualURL);
            fileDetails.setTitle(title);
            fileDetails.setPublishedDate(String.valueOf(publishedDate));
            fileDetails.isDetailsBoltExec(true);
            TweetDetailsManager.addDetails(title, fileDetails);
        }
        this.write(title);
        System.out.println(title);
    }

    /* (non-Javadoc)
     * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     * This method executes when instantiate created for this bolt.
     * It creates user_tweets.csv file with the template.
     */
    @Override
    public void prepare(Map arg0, TopologyContext arg1,
                        OutputCollector collector) {
        try {
            file = new File("user_tweets.csv");
            fileWriter = new FileWriter(file.getAbsoluteFile(), true);
            StringBuilder sb = new StringBuilder();
            sb.append("screen_name" + "," + "user_name" + "," + "status_id"
                    + "," + "url" + "," + "actual_url" + "," + "title"
                    + "," + "published_date" + "," + "retweeted" + ","
                    + "retweet_screen_name" + "," + "retweet_user_name");
            sb.append("\n");
            fileWriter.write(sb.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @param uniqueId
     * This method writes details in to file if all the tuples processed from the both bolts.
     */
    public void write(String uniqueId) {
        FileDetails fileDetails = TweetDetailsManager.get(uniqueId);
        if (fileDetails != null) {
            if (fileDetails.isWritable()) {
                try {
                    BufferedWriter bufferedWriter = new BufferedWriter(
                            fileWriter);
                    System.out.println("final content writing into blog :::: "+fileDetails.toString());
                    bufferedWriter.write(fileDetails.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                    TweetDetailsManager.remove(uniqueId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}