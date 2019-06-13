import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream tweetStream;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("status"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.queue = new LinkedBlockingQueue<Status>();
        UserStreamListener listener = new UserStreamListener() {

            @Override
            public void onException(Exception e) {

            }

            @Override
            public void onDeletionNotice(long l, long l1) {

            }

            @Override
            public void onFriendList(long[] longs) {

            }

            @Override
            public void onFavorite(User user, User user1, Status status) {

            }

            @Override
            public void onUnfavorite(User user, User user1, Status status) {

            }

            @Override
            public void onFollow(User user, User user1) {

            }

            @Override
            public void onDirectMessage(DirectMessage directMessage) {

            }

            @Override
            public void onUserListMemberAddition(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListMemberDeletion(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListSubscription(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListUnsubscription(User user, User user1, UserList userList) {

            }

            @Override
            public void onUserListCreation(User user, UserList userList) {

            }

            @Override
            public void onUserListUpdate(User user, UserList userList) {

            }

            @Override
            public void onUserListDeletion(User user, UserList userList) {

            }

            @Override
            public void onUserProfileUpdate(User user) {

            }

            @Override
            public void onBlock(User user, User user1) {

            }

            @Override
            public void onUnblock(User user, User user1) {

            }

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

        };

        ConfigurationBuilder cbuilder = new ConfigurationBuilder();
        cbuilder.setOAuthConsumerKey("JBAbc7FNeim1R3HjoagrTJTcO");
        cbuilder.setOAuthConsumerSecret("V61L1iZX5XjLWPB5hP1R5aN8wrIag6i6Ko4PQLUm0yk5a5a12q");
        cbuilder.setOAuthAccessToken("1121726044229328896-Bece1mGPP4osyYqxLEXVxjfGuEt4rb");
        cbuilder.setOAuthAccessTokenSecret("s5sjsYWHVoIukjfdGPYsSP270w7160BO6If9YrE7DbO50");

        this.tweetStream = new TwitterStreamFactory(cbuilder.build()).getInstance();
        this.tweetStream.addListener(listener);

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
        // Status object from the queue.
        Status status = queue.poll();
        if(status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

}
