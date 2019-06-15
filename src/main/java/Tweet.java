public class Tweet {

    private String text;
    private String date;
    private String timestamp;

    public Tweet(String text, String date, String timestamp) {

        this.text = text;
        this.date = date;
        this.timestamp = timestamp;

    }

    public String getText() {
        return this.text;
    }

    public String getDate() {
        return this.date;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

}
