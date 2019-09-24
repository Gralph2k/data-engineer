package ru.gralph2k.de;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.gralph2k.de.parser.PaperParser;
import ru.gralph2k.de.parser.PaperParserPresidential2018;
import ru.gralph2k.de.producers.PapersDummyProducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.testng.Assert.*;

public class PapersProcessorTest {

    @BeforeMethod
    public void setUp() throws IOException {
        FileUtils.forceMkdir(Paths.get("./src/test/resources/Processed").toFile());
        FileUtils.forceMkdir(Paths.get("./src/test/resources/Error").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/Processed").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/Error").toFile());
        FileUtils.copyDirectory(Paths.get("./src/test/resources/Template/").toFile(), Paths.get("./src/test/resources/Source").toFile());
    }


    @Test
    public void testProcessPapers() throws IOException {
        PapersDummyProducer dummyProducer = new PapersDummyProducer();
        PaperParser paperParser = new PaperParserPresidential2018();

        PapersProcessor papersProcessor = new PapersProcessor("./src/test/resources/Source",dummyProducer,paperParser,"testTopic");
        int rows = papersProcessor.processPapers();
        assertEquals(rows,3100);
    }
}