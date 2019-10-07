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
    private Integer delay = 0;

    public PapersProcessor(String sourceDir, PapersProducer papersProducer, PaperType paperType, String topic, Integer delay) throws IOException {
        this.papersProducer = papersProducer;
        this.fileHelper = FileHelper.getInstance(sourceDir, papersProducer);
        this.paperType = paperType; //TODO лучше передавать класс, а не объект. Разобраться как это делать
        this.topic = topic;
        this.delay = delay;

        log.info("PapersProcessor created {}\n{}\n{}\n{}", sourceDir, papersProducer.getClass().toString(), paperType.getClass().toString(), topic);
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
                            fileHelper.writeError(paper.getName(), line, ex.getMessage());
                            log.error("Error while parse {} value {}\n{}", this.paperType.getClass(), line, ex.getMessage());
                        }
                    }
                    fileHelper.movePaperToProcessed(paper);
                    log.info("File {} read successfully. Parsed lines {}, errors {}", paper.toString(), success, errors);
                } catch (IOException ex) {
                    log.error("Failed to read {} \n", paper.toString(), ex.getStackTrace());
                }
                //Sleep before processing next file to emulate real life
                try {
                    Thread.sleep(1000 * delay);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
        log.info("Processed lines {} in files {}", lines, files);
        return lines;
    }

    public static void main(String[] args) {
        try {
            log.info("Started. \nArgs.count={}", args.length);
            String propertiesFileName = "election.properties";
            if (args.length > 1) {
                log.info("Initializing producer, sleeping for 30 seconds to let Kafka startup");
                Thread.sleep(30000);
                propertiesFileName = args[1];
            }

            ElectionsProperties properties = ElectionsProperties.getInstance(propertiesFileName);

            PapersProducer papersProducer = ProducerFactory.getInstance(properties.getProducerName(), properties);
            PaperType paperType = PaperTypeFactory.getInstance(properties.getPaperTypeName(), null);

            new PapersProcessor(properties.getSourceDir(), papersProducer, paperType, properties.getTopic(), properties.getProducerProduceDelaySeconds()).processPapers();



        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log.info("Finished");
    }
}
