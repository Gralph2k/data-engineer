package ru.gralph2k.de.parser;

//Формат протокола на президентских выборах 2018
public class PaperParserPresidential2018 implements PaperParser {

    private int ps_id;
    private String region_name;
    private String subregion_name;
    private int Baburin;
    private int Grudinin;
    private int Zhirinovskiy;
    private int Putin;
    private int Sobchak;
    private int Uraikin;
    private int Titov;
    private int papers_in_boxes;
    private int valid_papers;
    private int voters;
    private int papers_portable;
    private int papers_inside;
    private int papers_outside;
    private int papers_advance;
    private int papers_excessive;

    public void parse(String source) {
        String[] column = source.split(",");
        ps_id = Integer.parseInt(column[0]);
        region_name = column[1];
        subregion_name = column[2];
        Baburin = Integer.parseInt(column[3]);
        Grudinin = Integer.parseInt(column[4]);
        Zhirinovskiy = Integer.parseInt(column[5]);
        Putin = Integer.parseInt(column[6]);
        Sobchak = Integer.parseInt(column[7]);
        Uraikin = Integer.parseInt(column[8]);
        papers_in_boxes = Integer.parseInt(column[9]);
        valid_papers = Integer.parseInt(column[10]);
        voters = Integer.parseInt(column[11]);
        papers_portable = Integer.parseInt(column[12]);
        papers_inside = Integer.parseInt(column[13]);
        papers_outside = Integer.parseInt(column[14]);
        papers_advance = Integer.parseInt(column[15]);
        papers_excessive = Integer.parseInt(column[16]);
    }

    public String getKey() {
        return String.valueOf(getPs_id()) + "_" + getRegion_name();
    }

    public int getPs_id() {
        return ps_id;
    }

    public void setPs_id(int ps_id) {
        this.ps_id = ps_id;
    }

    public String getRegion_name() {
        return region_name;
    }

    public String getSubregion_name() {
        return subregion_name;
    }

    public int getBaburin() {
        return Baburin;
    }

    public int getGrudinin() {
        return Grudinin;
    }

    public int getZhirinovskiy() {
        return Zhirinovskiy;
    }

    public int getPutin() {
        return Putin;
    }

    public int getSobchak() {
        return Sobchak;
    }

    public int getUraikin() {
        return Uraikin;
    }

    public int getTitov() {
        return Titov;
    }

    public int getPapers_in_boxes() {
        return papers_in_boxes;
    }

    public int getValid_papers() {
        return valid_papers;
    }

    public int getVoters() {
        return voters;
    }

    public int getPapers_portable() {
        return papers_portable;
    }

    public int getPapers_inside() {
        return papers_inside;
    }

    public int getPapers_outside() {
        return papers_outside;
    }
    public int getPapers_advance() {
        return papers_advance;
    }

    public int getPapers_excessive() {
        return papers_excessive;
    }

}
