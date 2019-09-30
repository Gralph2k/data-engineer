package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType;
import ru.gralph2k.de.producers.PapersProducer;
import ru.gralph2k.de.producers.ProducerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PapersProcessor {
    private static final Logger log = LoggerFactory.getLogger(PapersProcessor.class);

    private FileHelper fileHelper;
    private PapersProducer papersProducer;
    private PaperType paperType;
    private String topic;

    public PapersProcessor(String sourceDir, PapersProducer papersProducer, PaperType paperType, String topic) throws IOException {
        this.papersProducer = papersProducer;
        this.fileHelper = FileHelper.getInstance(sourceDir, papersProducer);
        this.paperType = paperType; //TODO лучше передавать класс, а не объект. Разобраться как это делать
        this.topic = topic;
    }

    public Integer processPapers() {
        int lines = 0;
        int files = 0;
        File[] papers = fileHelper.papersList();
        if (papers != null) {
            log.info("Find {} files", papers.length);
            for (File paper : papers) {
                try {
                    int success = 0;
                    int errors = 0;
                    files++;
                    List<String> content = fileHelper.readPaper(paper);
                    for (String line : content) {
                        try {
                            lines++;
                            if (this.paperType.parse(line)) {
                                papersProducer.send(topic, this.paperType.key(), this.paperType);
                                success++;
                            }
                        } catch (Exception ex) {
                            errors++;
                            fileHelper.writeError(paper.getName(), line, ex.getStackTrace()[0]);
                            log.error("Error while parse {} value {}\n{}", this.paperType.getClass(), line, ex.getMessage());
                        }
                    }
                    fileHelper.movePaperToProcessed(paper);
                    log.info("File {} read successfully. Parsed lines {}, errors {}", paper.toString(), success, errors);
                } catch (IOException ex) {
                    log.error("Failed to read {} \n", paper.toString(), ex.getStackTrace());
                }
            }
        }
        log.info("Processed lines {} in files {}", lines, files);
        return lines;
    }

    public static void main(String[] args) throws InterruptedException {
        log.info("Started. \nArgs.count={}", args.length);
        log.info("Initializing producer, sleeping for 30 seconds to let Kafka startup");
        Thread.sleep(30000);
        for (String arg : args) {
            log.info(arg);
        }
        String sourceDir = "./Data/Source";
        String producerName = "PapersKafkaProducer";
        String paperName = "PaperType_Presidential2018";
        String topic = "Presidential2018";

        if (args.length >= 1) {
            sourceDir = args[1];
        }
        if (args.length >= 2) {
            producerName = args[2];
        }
        if (args.length >= 3) {
            paperName = args[3];
        }
        if (args.length >= 4) {
            topic = args[4];
        }

        try {
            PapersProducer papersProducer = ProducerFactory.getInstance(producerName);
            log.info("papersProducer: {}", papersProducer.getClass().getName());
            PaperType paperType = PaperTypeFactory.getInstance(paperName);
            log.info("paperType: {}", paperType.getClass().getName());

            new PapersProcessor(sourceDir, papersProducer, paperType, topic).processPapers();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log.info("Finished");
    }
}
