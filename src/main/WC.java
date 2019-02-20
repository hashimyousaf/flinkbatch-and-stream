// some ordinary POJO
public class WC {
    public String word;
    public Integer count;

    public void setWord(String word) {
        this.word = word;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return word + " :: " + count;
    }

    public WC(){}
    public WC(String wrd, Integer i) {
        word = wrd;
        count = i;
    }
}