import java.util.ArrayList;

public class Utilities {

    /*** Since each word in the vocabulary .json file carries an array of 10 sentiments, we decide to keep things easy by
     * computing a single score starting from an array. Each sentiment contributes to the overall score with +1.0 if it's
     * positive and with -1.0 if it's negative. Knowing the standard format of the sentiments array, it's easy to
     * determine whether we need to add or subtract a value.
     */

    public static Double getScore(ArrayList<Double> values) {

        Double score = 0.0;

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

    /*** Since there isn't an effective method for comparing a String with the word contained in a VocabularyEntry
     * object, we decide to implement this method to check if a specific word is contained in a vocabulary.
     * @param word: String. The word for which you're looking a match.
     * @param words: ArrayList<VocabularyEntry>. The vocabulary.
     * @return: true if 'word' matches some word in 'words', false otherwise.
     */

    public static boolean containedIn(String word, ArrayList<VocabularyEntry> words) {

        for(int i = 0; i < words.size(); i++) {
            if(words.get(i).getWord().equals(word)) {
                return true;
            }
        }
        return false;

    }

    /*** After a match has been confirmed, given a word and a vocabulary, this method provides a way to get the
     * corresponding score.
     * @param word: String. The word for which you need the score.
     * @param words: ArrayList<VocabularyEntry>. The vocabulary.
     * @return: The corresponding score for the 'word' if the word is found, zero otherwise.
     */

    public static int returnScore(String word, ArrayList<VocabularyEntry> words) {

        for(int i = 0; i < words.size(); i++) {
            if(words.get(i).getWord().equals(word)) {
                return words.get(i).getScore();
            }
        }

        return 0;

    }

}
