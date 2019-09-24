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
        int processed = 0;
        File[] papers =  fileHelper.papersList();
        for (File paper:papers) {
            try {
                int success=0;
                int errors=0;
                List<String> content = fileHelper.readPaper(paper);
                for (String line: content) {
                        try {
                            processed++;
                            if (this.paper.parse(line.toString())) {
                                papersProducer.send(topic, this.paper.getKey(), this.paper.toString()); //TODO Send parsed object
                                success++;
                            }
                        } catch (Exception ex) {
                            errors++;
                            fileHelper.writeError(paper.getName(),line.toString());
                            log.error("Error while parse {}\n{}", line, ex.getMessage());
                        }
                }
                fileHelper.movePaperToProcessed(paper);
                log.info("File {} read successfully. Parsed lines {}, errors {}",paper.toString(), success, errors);
            } catch (IOException ex) {
                log.error("Failed to read {} \n",paper.toString(),ex.getStackTrace());
            }
        }
        log.info("Processed lines {}",processed);
        return processed;
    }

    public static void main(String[] args) throws IOException{
        if (args.length == 4 ) {
            String sourceDir = args[0];
            PapersProducer papersProducer = ProducerFactory.getInstance(args[1]);
            Paper paper = ParserFactory.getInstance(args[2]);
            String topic = args[3];
            PapersProcessor papersProcessor = new PapersProcessor(sourceDir, papersProducer, paper, topic);
            papersProcessor.processPapers();
        }
    }
}
