package ru.gralph2k.de.paperTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.DbHelper;

import java.sql.ResultSet;
import java.sql.SQLException;

//Формат протокола на президентских выборах 2018
//TODO Разделить логику парсера и работу с базой данных
public class PaperType_Presidential2018 extends PaperType {
    private static final Logger log = LoggerFactory.getLogger(PaperType_Presidential2018.class);

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

    private Integer protocols;
    private Integer IKs;
    private Integer regions;

    private static final String FIELDS_TYPES =
        "ID VARCHAR(500) NOT NULL," +
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
        " registered timestamp";

    private static final String FIELDS = "ID,ps_id,region_name,subregion_name," +
        "baburin,grudinin,zhirinovskiy,putin,sobchak,uraikin,titov," +
        "papers_in_boxes,valid_papers,voters,papers_portable,papers_inside,papers_outside,papers_advance,papers_excessive,registered";

    private static final String SOURCE_TABLE = "Presidential2018_Source";
    private static final String CLEAN_TABLE = "Presidential2018_Clean";

    public PaperType_Presidential2018() {}
    public PaperType_Presidential2018(DbHelper dbHelper) {
        super(dbHelper);
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

    @Override
    public void prepare() {
        dbHelper.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (%s)",SOURCE_TABLE,FIELDS_TYPES));
        dbHelper.executeUpdate(String.format("CREATE TABLE IF NOT EXISTS %s (%s)",CLEAN_TABLE,FIELDS_TYPES));
    }

    @Override
    public void save() {
        String sql = String.format(
            "INSERT INTO %s (%s) "
                + "VALUES ('%s', %d, '%s', '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, now() );"
            ,SOURCE_TABLE
            ,FIELDS
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
        dbHelper.executeUpdate(sql);
    }

    public void clean(){
        dbHelper.executeUpdate(String.format("TRUNCATE TABLE %s",CLEAN_TABLE));
        String sql = String.format(
            "INSERT INTO %s\n" +
            "\tSELECT %s \n" +
            "      FROM (\n" +
            "      \tSELECT %s\n" +
            "      \t     ,ROW_NUMBER() OVER (PARTITION BY ID ORDER BY REGISTERED DESC) RN\n" +
            "      \t  FROM %s\n" +
            "      ) A\n" +
            "      WHERE RN=1"
            ,CLEAN_TABLE,FIELDS,FIELDS,SOURCE_TABLE);
        dbHelper.executeUpdate(sql);
    }

    public ResultSet aggregate() {
        String sql = String.format(
            "SELECT COUNT(1) Protocols\n" +
                "      ,COUNT(DISTINCT ID) IKs\n" +
                "      ,COUNT(DISTINCT region_name) regions\n" +
                "      ,COALESCE(SUM(baburin),0) baburin\n" +
                "      ,COALESCE(SUM(grudinin),0) grudinin\n" +
                "      ,COALESCE(SUM(zhirinovskiy),0) zhirinovskiy\n" +
                "      ,COALESCE(SUM(putin),0) putin\n" +
                "      ,COALESCE(SUM(sobchak),0) sobchak\n" +
                "      ,COALESCE(SUM(uraikin),0) uraikin\n" +
                "      ,COALESCE(SUM(titov),0) titov\n" +
                "      ,COALESCE(SUM(papers_in_boxes),0) papers_in_boxes\n" +
                "      ,COALESCE(SUM(valid_papers),0) valid_papers\n" +
                "      ,COALESCE(SUM(voters),0) voters\n" +
                "      ,COALESCE(SUM(papers_portable),0) papers_portable\n" +
                "      ,COALESCE(SUM(papers_inside),0) papers_inside\n" +
                "      ,COALESCE(SUM(papers_outside),0) papers_outside\n" +
                "      ,COALESCE(SUM(papers_advance),0) papers_advance\n" +
                "      ,COALESCE(SUM(papers_excessive),0) papers_excessive\n" +
                "      ,MAX(registered) Max_Registered\n" +
                "  FROM %s", CLEAN_TABLE);
        return dbHelper.executeQuery(sql);
    }

    public boolean check(ResultSet resultSet){
        try {
            if (!resultSet.next()) {
                log.info("Ошибка! Нет данных");
                return true;
            }
            this.IKs = resultSet.getInt("IKs");
            this.regions = resultSet.getInt("regions");
            this.baburin = resultSet.getInt("baburin");
            this.grudinin = resultSet.getInt("grudinin");
            this.zhirinovskiy = resultSet.getInt("zhirinovskiy");
            this.putin = resultSet.getInt("putin");
            this.sobchak = resultSet.getInt("sobchak");
            this.uraikin = resultSet.getInt("uraikin");
            this.titov = resultSet.getInt("titov");
            this.papers_in_boxes = resultSet.getInt("papers_in_boxes");
            this.valid_papers = resultSet.getInt("valid_papers");
            this.voters = resultSet.getInt("voters");
            this.papers_portable = resultSet.getInt("papers_portable");
            this.papers_inside = resultSet.getInt("papers_inside");
            this.papers_outside = resultSet.getInt("papers_outside");
            this.papers_advance = resultSet.getInt("papers_advance");
            this.papers_excessive = resultSet.getInt("papers_excessive");

            Integer totalVotes = baburin+grudinin+zhirinovskiy+putin+sobchak+uraikin+titov;
            if (totalVotes>papers_in_boxes) {
                log.info("Ошибка! Сумма голосов за кандидатов ({}) превышает общее число голосов ({}) ",totalVotes,papers_in_boxes);
                //TODO return false;
            }

            if (papers_in_boxes>voters) {
                log.info("Ошибка! Сумма голосов за кандидатов ({}) превышает число избирателей ({})", papers_in_boxes, voters);
                return false;
            }
            return true;

        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return false;
    }

    public void showResult(ResultSet resultSet){
        try {
            resultSet.first();
            Double voters_percent =  100.0*valid_papers/voters;
            Integer totalVotes = baburin+grudinin+zhirinovskiy+putin+sobchak+uraikin+titov;

            String text=String.format(
                "---------------------------\n"+
                "Бабурин:       %.2f %% \t(%10d)\n"+
                "Грудинин:      %.2f %% \t(%10d)\n"+
                "Жириновский:   %.2f %% \t(%10d)\n"+
                "Путин:         %.2f %% \t(%10d)\n"+
                "Собчак:        %.2f %% \t(%10d)\n"+
                "Урайкин:       %.2f %% \t(%10d)\n"+
                "Титов:         %.2f %% \t(%10d)\n"+
                "\n"+
                "Итого          %.2f %% \t(%10d)\n"+
                "Явка,%%        %g"
                ,100.0*baburin/valid_papers, baburin
                ,100.0*grudinin/valid_papers, grudinin
                ,100.0*zhirinovskiy/valid_papers, zhirinovskiy
                ,100.0*putin/valid_papers, putin
                ,100.0*sobchak/valid_papers, sobchak
                ,100.0*uraikin/valid_papers, uraikin
                ,100.0*titov/valid_papers, titov
                ,100.0*totalVotes/valid_papers, totalVotes
                ,voters_percent);
            System.out.println(text);

        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
