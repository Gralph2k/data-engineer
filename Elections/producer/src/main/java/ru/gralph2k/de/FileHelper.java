package ru.gralph2k.de;

import com.opencsv.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.producers.PapersProducer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileHelper {

    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    private Path sourceDir;
    private Path processedDir;
    private Path errorDir;
    private PapersProducer papersProducer;

    private static FileHelper single_instance;

    private FileHelper(String sourceDir, PapersProducer papersProducer) throws IOException {
        this.sourceDir = Paths.get(sourceDir);
        this.papersProducer = papersProducer;
        this.processedDir = Paths.get(this.sourceDir.getParent().toString(), "Processed");
        this.errorDir = Paths.get(this.sourceDir.getParent().toString(), "Error");
        Files.createDirectories(this.processedDir);
        Files.createDirectories(this.errorDir);
    }

    public static FileHelper getInstance(String sourceDir, PapersProducer kafkaProducer) throws IOException{
        if (single_instance == null) {
            single_instance = new FileHelper(sourceDir, kafkaProducer);
        }
        return single_instance;
    }

    public Path getSourceDir() {
        return sourceDir;
    }

    public Path getProcessedDir() {
        return processedDir;
    }

    public Path getErrorDir() {
        return errorDir;
    }

    public PapersProducer getKafkaProducer(){
        return papersProducer;
    }

    public File[] papersList() {
        return new File(sourceDir.toUri()).listFiles((d, name) -> name.endsWith(".csv"));
    }

    void movePaper(File file, Path toDir)  {
        Path destinationPath = toDir.resolve(Paths.get(file.getName()));
        try {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Isn't file: %s", file.toString()));
            }
            Files.move(Paths.get(file.toURI()), destinationPath, REPLACE_EXISTING);
            log.info("File {} moved to {} successfully", file.toURI(), destinationPath);
        } catch (IOException ex) {
            log.error("Failed to move {} to {}\n",file.toURI(), destinationPath,ex.getMessage());
        }
    }

    public void writeError(String fileName, String line) throws IOException {
        Path filePath = getErrorDir().resolve(fileName);
        Files.write(Paths.get(filePath.toString()), line.getBytes(), StandardOpenOption.APPEND);
    }

    public void movePaperToError(File file) {
        movePaper(file, errorDir);
    }

    public void movePaperToProcessed(File file) {
        movePaper(file, processedDir);
    }

    List<List<String>> readPaper(File file) throws IOException {
        List<List<String>> records = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(file.getCanonicalFile()))) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        }
        return records;
    }


}
