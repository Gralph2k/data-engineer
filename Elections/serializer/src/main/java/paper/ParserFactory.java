package ru.gralph2k.de.paper;

public class ParserFactory {
    public static Paper getInstance(String name) {
        if (name.equals("PaperPresidential2018")) {
            return new PaperPresidential2018();
        } else {
            return null;
        }
    }
}
