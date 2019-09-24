package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.parser.PaperParser;
import ru.gralph2k.de.producers.PapersProducer;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PapersProcessor {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    private FileHelper fileHelper;
    private PapersProducer papersProducer;
    private PaperParser paperParser;
    private String topic;

    public PapersProcessor(String sourceDir, PapersProducer papersProducer, PaperParser paperParser, String topic)  throws IOException{
        this.papersProducer = papersProducer;
        this.fileHelper = FileHelper.getInstance(sourceDir,papersProducer);
        this.paperParser = paperParser; //TODO лучше передавать класс, а не объект. Разобраться как это делать
        this.topic=topic;
    }

    public int processPapers() {
        File[] papers =  fileHelper.papersList();
        int counter=0;
        int errors=0;
        for (File paper:papers) {
            try {
                List<List<String>> content = fileHelper.readPaper(paper);
                for (List<String> line: content) {
                        try {
                            paperParser.parse(line.toString());
                            papersProducer.send(topic, paperParser.getKey(), paperParser.toString()); //TODO Send parsed object
                            counter++;
                        } catch (Exception ex) {
                            errors++;
                            fileHelper.writeError(paper.getName(),line.toString());
                            log.error("Error while parse {}\n{}",line,ex.getStackTrace());
                        }
                }
                fileHelper.movePaperToProcessed(paper);
                log.info("File {} read successfully. Parsed lines {}, errors {}",paper.toString(), content, errors);
            } catch (IOException ex) {
                log.error("Failed to read {} \n",paper.toString(),ex.getMessage());
                fileHelper.movePaperToError(paper);
            }
        }
        return counter;
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            if (args[0].equals("test")) {
                // load test parameters
            } else if (args[0].equals("production")) {
                // load production parameters
            }
        }
    }
}
