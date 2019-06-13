import java.util.Map;
import java.util.HashMap;

public class TweetDetailsManager {
    // intialize map object.
    private static Map<String,FileDetails> cacheManager = new HashMap<String, FileDetails>();

    /**
     * @param key
     * @return FileDetails
     * get value by passing key.
     */
    public static FileDetails get(String key) {
        return cacheManager.get(key);
    }

    /**
     * Intialize map object
     */
    public static void init() {
        cacheManager = new HashMap<String,FileDetails>();
    }

    /**
     * @param key
     * @param fileDetails
     * Add elements to map.
     */
    public static void addDetails(String key, FileDetails fileDetails) {
        cacheManager.put(key, fileDetails);
    }

    /**
     * @param key
     * Remove map element using key.
     */
    public static void remove(String key) {
        cacheManager.remove(key);
    }

}