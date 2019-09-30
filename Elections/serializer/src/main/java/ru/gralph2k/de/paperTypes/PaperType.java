package ru.gralph2k.de.paperTypes;

import ru.gralph2k.de.DbHelper;

import java.sql.ResultSet;

abstract public class PaperType {

    DbHelper dbHelper;

    abstract public String key();

    abstract public boolean parse(String source);

    public abstract void prepare();

    public abstract void save();

    public abstract void clean();

    public abstract ResultSet aggregate();

    public abstract boolean check(ResultSet resultSet);

    public abstract void showResult(ResultSet resultSet);

    public PaperType() {};

    public PaperType(DbHelper dbHelper) {
        this.dbHelper=dbHelper;
    }

    public DbHelper getDbHelper() {
        return dbHelper;
    }

    public void setDbHelper(DbHelper dbHelper) {
        this.dbHelper = dbHelper;
    }
}
