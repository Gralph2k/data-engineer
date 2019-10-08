package ru.gralph2k.de.paperTypes;

import java.sql.ResultSet;
import java.util.List;

abstract public class PaperType {

    abstract public String key();

    abstract public boolean parse(String source);

    public abstract List<String> prepareCommand();

    public abstract String saveCommand();

    public abstract List<String> cleanCommand();

    public abstract String aggregateCommand();

    public abstract boolean check(ResultSet resultSet);

    public abstract void showResult(ResultSet resultSet);

    public PaperType() {
    }

    ;

    Integer parseToInt(String param) {
        if (param.isEmpty() || param.equals("\"\"")) {
            return null;
        } else {
            return Integer.parseInt(param);
        }
    }
}
