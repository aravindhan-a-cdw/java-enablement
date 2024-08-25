package model;

public class WordFrequency {
    private final String word;
    private final Integer count;
    public WordFrequency(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String toString() {
        return "WordFrequency{" + "word='" + word + "'" + ", count=" + count + "}";
    }
}
