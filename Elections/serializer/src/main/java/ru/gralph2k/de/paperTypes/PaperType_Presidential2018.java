package ru.gralph2k.de.paperTypes;

import ru.gralph2k.de.DbHelper;

import java.sql.SQLException;

//Формат протокола на президентских выборах 2018
public class PaperType_Presidential2018 extends PaperType {

    private Integer ps_id;
    private String region_name;
    private String subregion_name;
    private Integer baburin;
    private Integer grudinin;
    private Integer zhirinovskiy;
    private Integer putin;
    private Integer sobchak;
    private Integer uraikin;
    private Integer titov;
    private Integer papers_in_boxes;
    private Integer valid_papers;
    private Integer voters;
    private Integer papers_portable;
    private Integer papers_inside;
    private Integer papers_outside;
    private Integer papers_advance;
    private Integer papers_excessive;


    public PaperType_Presidential2018() {
    }

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
        if (ps_id == null) {
            throw new IllegalArgumentException("ps_id mustn't be empty");
        }
        if (region_name == null) {
            throw new IllegalArgumentException("region_name mustn't be empty");
        }

        subregion_name = column[2];
        baburin = parseToInt(column[3]);
        grudinin = parseToInt(column[4]);
        zhirinovskiy = parseToInt(column[5]);
        putin = parseToInt(column[6]);
        sobchak = parseToInt(column[7]);
        uraikin = parseToInt(column[8]);
        titov = parseToInt(column[10]);
        papers_in_boxes = parseToInt(column[10]);
        valid_papers = parseToInt(column[11]);
        voters = parseToInt(column[12]);
        papers_portable = parseToInt(column[13]);
        papers_inside = parseToInt(column[14]);
        papers_outside = parseToInt(column[15]);
        papers_advance = parseToInt(column[16]);
        papers_excessive = parseToInt(column[17]);
        return true;
    }

    Integer parseToInt(String param) {
        if (param.isEmpty() || param.equals("\"\"")) {
            return null;
        } else {
            return Integer.parseInt(param);
        }
    }

    public String key() {
        return String.valueOf(ps_id) + "__" + region_name;
    }

    public Integer getPs_id() {
        return ps_id;
    }

    public String getRegion_name() {
        return region_name;
    }

    public String getSubregion_name() {
        return subregion_name;
    }

    public Integer getBaburin() {
        return baburin;
    }

    public Integer getGrudinin() {
        return grudinin;
    }

    public Integer getZhirinovskiy() {
        return zhirinovskiy;
    }

    public Integer getPutin() {
        return putin;
    }

    public Integer getSobchak() {
        return sobchak;
    }

    public Integer getUraikin() {
        return uraikin;
    }

    public Integer getTitov() {
        return titov;
    }

    public Integer getPapers_in_boxes() {
        return papers_in_boxes;
    }

    public Integer getValid_papers() {
        return valid_papers;
    }

    public Integer getVoters() {
        return voters;
    }

    public Integer getPapers_portable() {
        return papers_portable;
    }

    public Integer getPapers_inside() {
        return papers_inside;
    }

    public Integer getPapers_outside() {
        return papers_outside;
    }

    public Integer getPapers_advance() {
        return papers_advance;
    }

    public Integer getPapers_excessive() {
        return papers_excessive;
    }

    public static void prepare(DbHelper helper) throws SQLException {
        //helper.execSql("DROP TABLE Presidential2018_Source");
        String sql =
            "CREATE TABLE IF NOT EXISTS Presidential2018_Source " +
                "(ID VARCHAR(500) NOT NULL," +
                " ps_id INT NOT NULL, " +
                " region_name VARCHAR(500) NOT NULL, " +
                " subregion_name VARCHAR(500) NOT NULL, " +
                " baburin INT, " +
                " grudinin INT, " +
                " zhirinovskiy INT, " +
                " putin INT, " +
                " sobchak INT, " +
                " uraikin INT, " +
                " titov INT, " +
                " papers_in_boxes INT, " +
                " valid_papers INT, " +
                " voters INT, " +
                " papers_portable INT, " +
                " papers_inside INT, " +
                " papers_outside INT, " +
                " papers_advance INT, " +
                " papers_excessive INT," +
                " registered timestamp" +
                ") ";
        helper.execSql(sql);
    }

    @Override
    public boolean save(DbHelper helper) throws SQLException {

        String sql = String.format(
            "INSERT INTO Presidential2018_Source (ID,ps_id,region_name,subregion_name," +
                "baburin,grudinin,zhirinovskiy,putin,sobchak,uraikin,titov," +
                "papers_in_boxes,valid_papers,voters,papers_portable,papers_inside,papers_outside,papers_advance,papers_excessive,registered) "
                + "VALUES ('%s', %d, '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, now() );"
            ,key()
            ,ps_id
            ,region_name
            ,subregion_name
            ,baburin
            ,grudinin
            ,zhirinovskiy
            ,putin
            ,sobchak
            ,uraikin
            ,titov
            ,papers_in_boxes
            ,valid_papers
            ,voters
            ,papers_portable
            ,papers_inside
            ,papers_outside
            ,papers_advance
            ,papers_excessive);
        helper.execSql(sql);
        return true;
    }

}
