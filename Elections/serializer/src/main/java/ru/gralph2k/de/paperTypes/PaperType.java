package ru.gralph2k.de.paperTypes;

abstract public class PaperType {

    abstract public String key();

    abstract public boolean parse(String source);
}
