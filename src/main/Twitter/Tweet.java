package main.Twitter;

public class Tweet {
    private String language;
    private String text;

    public String getLanguage() {
        return language;
    }

    public String getText() {
        return text;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return language + " :: " + text;
    }
}
