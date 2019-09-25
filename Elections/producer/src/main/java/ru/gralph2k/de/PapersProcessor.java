package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paper.Paper;
import ru.gralph2k.de.paper.ParserFactory;
import ru.gralph2k.de.producers.PapersProducer;
import ru.gralph2k.de.producers.ProducerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PapersProcessor {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    private FileHelper fileHelper;
    private PapersProducer papersProducer;
    private Paper paper;
    private String topic;

    public PapersProcessor(String sourceDir, PapersProducer papersProducer, Paper paper, String topic)  throws IOException{
        this.papersProducer = papersProducer;
        this.fileHelper = FileHelper.getInstance(sourceDir,papersProducer);
        this.paper = paper; //TODO лучше передавать класс, а не объект. Разобраться как это делать
        this.topic=topic;
    }

    public Integer processPapers() {
        int lines = 0;
        int files = 0;
        File[] papers =  fileHelper.papersList();
        if (papers!=null) {
            System.out.println(String.format("Find %d files", papers.length));
            for (File paper : papers) {
                try {
                    int success = 0;
                    int errors = 0;
                    files++;
                    List<String> content = fileHelper.readPaper(paper);
                    for (String line : content) {
                        try {
                            lines++;
                            if (this.paper.parse(line.toString())) {
                                papersProducer.send(topic, this.paper.getKey(), this.paper);
                                success++;
                            }
                        } catch (Exception ex) {
                            errors++;
                            fileHelper.writeError(paper.getName(), line.toString());
                            log.error("Error while parse {}\n{}", line, ex.getMessage());
                        }
                    }
                    fileHelper.movePaperToProcessed(paper);
                    log.info("File {} read successfully. Parsed lines {}, errors {}", paper.toString(), success, errors);
                } catch (IOException ex) {
                    log.error("Failed to read {} \n", paper.toString(), ex.getStackTrace());
                }
            }
        }
        log.info("Processed lines {} in files {}",lines,files);
        return lines;
    }

    public static void main(String[] args) throws IOException,InterruptedException{
        System.out.println("started. \nArgs.count="+args.length);
        System.out.println("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup");
        Thread.sleep(300);
        for (String arg:args) {
            System.out.println(arg);
        }
        String sourceDir = "./Data/Source";
        String producerName = "PapersDummyProducer";
        String paperName = "PaperPresidential2018";
        String topic = "Presidential2018";

        if (args.length>=1) {
            sourceDir = args[1];
        }
        if (args.length>=2) {
            producerName=args[2];
        }
        if (args.length>=3) {
            paperName = args[3];
        }
        if (args.length>=4) {
            topic = args[4];
        }

        try {
           new PapersProcessor(
                sourceDir,
                ProducerFactory.getInstance(producerName),
                ParserFactory.getInstance(paperName),
                topic)
               .processPapers();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("finshed");
    }
}
