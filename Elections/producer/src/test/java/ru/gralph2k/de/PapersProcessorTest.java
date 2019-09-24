package ru.gralph2k.de;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.gralph2k.de.paper.Paper;
import ru.gralph2k.de.paper.ParserFactory;
import ru.gralph2k.de.producers.PapersProducer;
import ru.gralph2k.de.producers.ProducerFactory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PapersProcessorTest {

    @BeforeMethod
    public void setUp() throws IOException {
        FileUtils.forceMkdir(Paths.get("./src/test/resources/Source").toFile());
        FileUtils.forceMkdir(Paths.get("./src/test/resources/Processed").toFile());
        FileUtils.forceMkdir(Paths.get("./src/test/resources/ErrorRows").toFile());
        FileUtils.forceMkdir(Paths.get("./src/test/resources/SuccessRows").toFile());

        FileUtils.cleanDirectory(Paths.get("./src/test/resources/Source").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/Processed").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/ErrorRows").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/SuccessRows").toFile());

        FileUtils.copyDirectory(Paths.get("./src/test/resources/Template/").toFile(), Paths.get("./src/test/resources/Source/").toFile());
    }

    @Test
    public void testProcessPapers() throws IOException {
        PapersProducer dummyProducer = ProducerFactory.getInstance("PapersDummyProducer");
        Paper paper = ParserFactory.getInstance("PaperPresidential2018");

        PapersProcessor papersProcessor = new PapersProcessor("./src/test/resources/Source", dummyProducer, paper, "testTopic");
        int rows = papersProcessor.processPapers();
        assertEquals(rows, 3100);

        assertTrue(FileUtils.listFiles(Paths.get("./src/test/resources/ErrorRows").toFile(),new String[] {"csv"}, false).size()==1);
        assertTrue(FileUtils.listFiles(Paths.get("./src/test/resources/Processed").toFile(),new String[] {"csv"}, false).size()==3);
        assertTrue(FileUtils.listFiles(Paths.get("./src/test/resources/Source").toFile(),new String[] {"csv"}, false).size()==0);
    }
}