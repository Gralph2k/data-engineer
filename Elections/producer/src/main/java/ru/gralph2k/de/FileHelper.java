package ru.gralph2k.de;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.producers.PapersProducer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileHelper {

    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    private Path sourceDir; //Папка с файлами для импорта
    private Path processedDir; //Папка с обработканными файлами
    private Path errorRowsDir; //Файлы, содержащие строки, которые не удалось распарсить
    private Path successRowsDir; //Файлы, содержашие удачно распаршеные строки
    private String filePrefix; //Префикс файлов со строками (дата+время запуска)

    private static FileHelper single_instance;

    private FileHelper(String sourceDir, PapersProducer papersProducer) throws IOException {
        this.sourceDir = Paths.get(sourceDir);
        String parent = this.sourceDir.getParent().toString();
        this.processedDir = Paths.get(parent,"Processed");
        this.errorRowsDir = Paths.get(parent,"ErrorRows");
        this.successRowsDir = Paths.get(parent,"SuccessRows");

        FileUtils.forceMkdir(processedDir.toFile());
        FileUtils.forceMkdir(errorRowsDir.toFile());
        FileUtils.forceMkdir(successRowsDir.toFile());

        filePrefix = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date());
    }

    public static FileHelper getInstance(String sourceDir, PapersProducer kafkaProducer) throws IOException {
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

    public Path getErrorRowsDir() {
        return errorRowsDir;
    }

    public Path getSuccessRowsDir() {
        return successRowsDir;
    }

    public File[] papersList() {
        System.out.println(String.format("Scan %s ",sourceDir.toString()));
        return new File(sourceDir.toUri()).listFiles((d, name) -> name.endsWith(".csv"));
    }

    void movePaper(File file, Path toDir) {
        Path destinationPath = toDir.resolve(Paths.get(file.getName()));
        try {
            if (!file.isFile()) {
                throw new IllegalArgumentException(String.format("Isn't file: %s", file.toString()));
            }
            Files.move(Paths.get(file.toURI()), destinationPath, REPLACE_EXISTING);
        } catch (IOException ex) {
            log.error("Failed to move {} to {}\n", file.toURI(), destinationPath, ex.getMessage());
        }
    }

    public void writeError(String fileName, String line) throws IOException {
        Path errorFile = getErrorRowsDir().resolve(filePrefix + "_" + fileName);
        FileUtils.writeStringToFile(errorFile.toFile(), line + System.lineSeparator(), "UTF-8", true);
    }

    public void writeSuccess(String fileName, String line) throws IOException {
        Path successFile = getSuccessRowsDir().resolve(filePrefix + "_" + fileName);
        FileUtils.writeStringToFile(successFile.toFile(), line + System.lineSeparator(), "UTF-8", true);
    }

    public void movePaperToProcessed(File file) {
        movePaper(file, processedDir);
    }

    List<String> readPaper(File file) throws IOException {
        return Files.readAllLines(file.toPath());
    }


}
