package ru.gralph2k.de.paper;

//Формат протокола на президентских выборах 2018
public class PaperPresidential2018 extends Paper {

    private Integer ps_id;
    private String region_name;
    private String subregion_name;
    private Integer Baburin;
    private Integer Grudinin;
    private Integer Zhirinovskiy;
    private Integer Putin;
    private Integer Sobchak;
    private Integer Uraikin;
    private Integer Titov;
    private Integer papers_in_boxes;
    private Integer valid_papers;
    private Integer voters;
    private Integer papers_portable;
    private Integer papers_inside;
    private Integer papers_outside;
    private Integer papers_advance;
    private Integer papers_excessive;

    PaperPresidential2018() {};

    /**
     * Заполняет поля объекта на основании входящей строки. Если прошло успено - возвращает true.
     * Если на входе header - возвращает false
     * При ошибке кастинга бросит exception
     */
    public boolean parse(String source) {
        String[] column = source.split(",");
        if (column[0].contains("ps_id")) {
            return false;
        }
        ps_id = parseToInt(column[0]);
        region_name = column[1];
        subregion_name = column[2];
        Baburin = parseToInt(column[3]);
        Grudinin = parseToInt(column[4]);
        Zhirinovskiy = parseToInt(column[5]);
        Putin = parseToInt(column[6]);
        Sobchak = parseToInt(column[7]);
        Uraikin = parseToInt(column[8]);
        papers_in_boxes = parseToInt(column[9]);
        valid_papers = parseToInt(column[10]);
        voters = parseToInt(column[11]);
        papers_portable = parseToInt(column[12]);
        papers_inside = parseToInt(column[13]);
        papers_outside = parseToInt(column[14]);
        papers_advance = parseToInt(column[15]);
        papers_excessive = parseToInt(column[16]);
        return true;
    }
    
    Integer parseToInt(String param) {
        if (param.isEmpty()||param.equals("\"\"")) {
            return null;
        } else {
            return Integer.parseInt(param);
        }
    }

    public String getKey() {
        return String.valueOf(ps_id) + "_" + region_name;
    }

   

}
