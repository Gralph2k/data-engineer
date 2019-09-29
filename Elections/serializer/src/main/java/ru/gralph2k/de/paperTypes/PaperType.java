package ru.gralph2k.de.paperTypes;

import ru.gralph2k.de.DbHelper;

import java.sql.SQLException;

abstract public class PaperType {

    abstract public String key();

    abstract public boolean parse(String source);

    abstract boolean save(DbHelper helper)  throws SQLException;

    static void prepare(DbHelper helper)  throws SQLException {};
}
