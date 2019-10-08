package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType;

import java.sql.ResultSet;

public class PapersAggregator {
    private static final Logger log = LoggerFactory.getLogger(PapersAggregator.class);

    private DbHelper dbHelper;
    private String paperTypeClass;
    private Integer delayMs;

    public PapersAggregator(String paperTypeClass, String user, String password, String connectionString, Integer delayMs) {
        log.info("PapersAggregator created\npaperType:{}\nuser:{}\nconnectionString:{}", paperTypeClass, user, connectionString);
        this.paperTypeClass = paperTypeClass;
        this.dbHelper = new DbHelper(user, password, connectionString);
        this.delayMs = delayMs;
    }

    private void process() {
        PaperType paperType = PaperTypeFactory.getInstance(paperTypeClass);
        dbHelper.executeUpdate(paperType.prepareCommand());
        dbHelper.executeUpdate(paperType.cleanCommand());

        ResultSet resultSet = dbHelper.executeQuery(paperType.aggregateCommand());
        if (paperType.check(resultSet)) {
            paperType.showResult(resultSet);
        }
        try {
            Thread.sleep(delayMs * 1000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            log.info("Started. \nArgs.count={}", args.length);
            String propertiesFileName = "config/election.properties";
            if (args.length > 1) {
                log.info("Initializing aggregator, sleeping for 30 seconds to let postgres startup");
                Thread.sleep(40000);
                propertiesFileName = args[1];
            }

            ElectionsProperties properties = ElectionsProperties.getInstance(propertiesFileName);

            PapersAggregator papersAggregator = new PapersAggregator(
                properties.getPaperTypeName(),
                properties.getAggregatorPGUser(),
                properties.getAggregatorPGPassword(),
                properties.getAggregatorPGconnectionString(),
                properties.getAggregatorDelaySeconds());

            while (true) {
                papersAggregator.process();

            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log.info("Finished");
    }

}
