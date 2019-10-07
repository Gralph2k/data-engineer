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
        log.info("PapersAggregator created {}\n{}\n{}", paperTypeClass, user, connectionString);
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
        try {
            Thread.sleep(delayMs * 1000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            log.info("Started. \nArgs.count={}", args.length);
            String propertiesFileName = "election.properties";
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
                properties.getProducerProduceDelaySeconds());

            while (true) {
                papersAggregator.process();

            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log.info("Finished");
    }

}
