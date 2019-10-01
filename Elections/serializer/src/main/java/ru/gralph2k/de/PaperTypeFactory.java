package ru.gralph2k.de;

import ru.gralph2k.de.paperTypes.*;

public class PaperTypeFactory {
    public static PaperType getInstance(String name, DbHelper dbHelper) {
        if (name.equals("PaperType_Presidential_2018")) {
            return new PaperType_Presidential_2018(dbHelper);
        } else {
            return null;
        }
    }

    public static Class getClass(String name) {
        if (name.equals("PaperType_Presidential_2018")) {
            return PaperType_Presidential_2018.class;
        } else {
            return null;
        }
    }
}
