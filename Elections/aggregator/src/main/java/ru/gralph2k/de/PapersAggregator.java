package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType;

import java.sql.ResultSet;

public class PapersAggregator {
    private static final Logger log = LoggerFactory.getLogger(PapersAggregator.class);

    DbHelper dbHelper;
    String paperTypeClass;
    Integer delayMs;

    public PapersAggregator(String paperTypeClass, String user, String password, String connectionString, Integer delayMs) {
        this.paperTypeClass = paperTypeClass;
        this.dbHelper = new DbHelper(user, password, connectionString);
        this.delayMs = delayMs;
    }

    public void process() {
        PaperType paperType = PaperTypeFactory.getInstance(paperTypeClass, dbHelper);
        paperType.clean();
        ResultSet resultSet = paperType.aggregate();
        if (paperType.check(resultSet)) {
            paperType.showResult(resultSet);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        log.info("Started. \nArgs.count={}", args.length);
        if (args.length>0) {
            log.info("Initializing aggregator, sleeping for 30 seconds to let postgres startup");
            Thread.sleep(30000);
            for (String arg : args) {
                log.info(arg);
            }
        }

        String paperTypeClass = "PaperType_Presidential_2018";
        String user = "consumer";
        String password = "goto@Postgres1";
        String connectionString = "jdbc:postgresql://100.100.21.232:5432/Elections";
        Integer delayMs = 1000;

        //TODO Заменить на cli
        if (args.length >= 1) {
            paperTypeClass = args[1];
        }
        if (args.length >= 2) {
            user = args[2];
        }
        if (args.length >= 3) {
            password = args[3];
        }
        if (args.length >= 4) {
            connectionString = args[4];
        }

        if (args.length >= 5) {
            delayMs = Integer.parseInt(args[5]);
        }

        PapersAggregator papersAggregator = new PapersAggregator(paperTypeClass, user, password, connectionString,delayMs);

        while (true) {
            try {
                papersAggregator.process();
                Thread.sleep(delayMs);
            } catch (InterruptedException ex) {
            }
        }
    }

}
