package ru.gralph2k.de;

import ru.gralph2k.de.paperTypes.*;

public class ParserFactory {
    public static PaperType getInstance(String name) {
        if (name.equals("PaperType_Presidential2018")) {
            return new PaperType_Presidential2018();
        } else {
            return null;
        }
    }
}
