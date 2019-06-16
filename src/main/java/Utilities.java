import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Utilities {

    public static int getScore(ArrayList<Float> values) {

        int score = 0;

        for(int i = 0; i < values.size(); i++) {
            if(i == 1 || i == 2 || i == 4 || i == 5 || i == 7) {
                score -= values.get(i);
            }
            else {
                score += values.get(i);
            }
        }

        return score;

    }

    public static boolean containedIn(String word, ArrayList<VocabularyEntry> words) {

        for(int i = 0; i < words.size(); i++) {
            if(words.get(i).getWord().equals(word)) {
                return true;
            }
        }
        return false;

    }

    public static int returnScore(String word, ArrayList<VocabularyEntry> words) {

        for(int i = 0; i < words.size(); i++) {
            if(words.get(i).getWord().equals(word)) {
                return words.get(i).getScore();
            }
        }

        return 0;

    }

}
