public class VocabularyEntry {

    private String word;
    private int score;

    /*** This class has been created only because it provides a simple way to store the dictionary.
     * @param word: String. Each key of a .json file is a word we can classify.
     * @param score: ArrayList<Double>. Each word in the vocabulary comes with a sentiments array. It has a length of
     *             10 and each entry represents a sentiment. There are 5 positive and 5 negative sentiments, combined to
     *             give a single integer score as a result.
     */

    public VocabularyEntry(String word, int score) {

        this.word = word;
        this.score = score;

    }

    public String getWord() {
        return this.word;
    }

    public int getScore() {
        return this.score;
    }

}
