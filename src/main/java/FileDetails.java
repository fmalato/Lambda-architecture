public class FileDetails {
    private String screenName;
    private String userName;
    private String statusID;
    private String url;
    private String actualURL;
    private String title;
    private String publishedDate;
    private String isRetweet ;
    private String retweetScreenName;
    private String retweetUserName;
    private boolean isDetailsBoltExec = false;
    private boolean isRetweetBoltExec = false;

    /**
     * @param screenName - the screenName to set.
     */
    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }
    /**
     * @param userName - the userName to set.
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }
    /**
     * @param statusID - the statusID to set.
     */
    public void setStatusID(String statusID) {
        this.statusID = statusID;
    }
    /**
     * @param url - the url to set.
     */
    public void setUrl(String url) {
        this.url = url;
    }
    /**
     * @param actualURL - the actualURL to set.
     */
    public void setActualURL(String actualURL) {
        this.actualURL = actualURL;
    }
    /**
     * @param title - the title to set.
     */
    public void setTitle(String title) {
        this.title = title;
    }
    /**
     * @param publishedDate - the publishedDate to set.
     */
    public void setPublishedDate(String publishedDate) {
        this.publishedDate = publishedDate;
    }
    /**
     * @param isRetweet - the isRetweet to set.
     */
    public void setIsRetweet(String isRetweet) {
        this.isRetweet = isRetweet;
    }
    /**
     * @param retweetScreenName - the retweetScreenName to set.
     */
    public void setRetweetScreenName(String retweetScreenName) {
        this.retweetScreenName = retweetScreenName;
    }
    /**
     * @param retweetUserName - the retweetUserName to set.
     */
    public void setRetweetUserName(String retweetUserName) {
        this.retweetUserName = retweetUserName;
    }

    /**
     * @param isDetailsBoltExec - the isDetailsBoltExec to set.
     */
    public void isDetailsBoltExec(boolean isDetailsBoltExec) {
        this.isDetailsBoltExec = isDetailsBoltExec;
    }

    /**
     * @param isRetweetBoltExec - the isRetweetBoltExec to set.
     */
    public void isRetweetBoltExec(boolean isRetweetBoltExec) {
        this.isRetweetBoltExec = isRetweetBoltExec;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     * override toString() method.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\""+this.screenName+"\",");
        sb.append("\""+this.userName+"\",");
        sb.append("\""+this.statusID+"\",");
        sb.append("\""+this.url+"\",");
        sb.append("\""+this.actualURL+"\",");
        sb.append("\""+this.title+"\",");
        sb.append("\""+this.publishedDate+"\",");
        sb.append("\""+this.isRetweet +"\",");
        sb.append("\""+this.retweetScreenName+"\",");
        sb.append("\""+this.retweetUserName+"\"");
        return sb.toString();
    }
    /**
     * This method checks whether both bolts are processed or not.
     * @return boolean
     */
    public boolean isWritable() {
        if(this.isDetailsBoltExec && this.isRetweetBoltExec) {
            return true;
        }
        return false;
    }
}