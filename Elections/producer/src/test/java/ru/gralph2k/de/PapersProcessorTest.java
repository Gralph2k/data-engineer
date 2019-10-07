package ru.gralph2k.de;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.gralph2k.de.paperTypes.PaperType;
import ru.gralph2k.de.producers.PapersProducer;
import ru.gralph2k.de.producers.ProducerFactory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.testng.Assert.assertEquals;

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
    public void testDummyProcessPapers() throws IOException {
        PapersProducer dummyProducer = ProducerFactory.getInstance("PapersDummyProducer", ElectionsProperties.getInstance("../resources/election.properties"));
        PaperType paperType = PaperTypeFactory.getInstance("PaperType_Presidential_2018", null);

        PapersProcessor papersProcessor = new PapersProcessor("./src/test/resources/Source", dummyProducer, paperType, "testTopic", 0);
        int rows = papersProcessor.processPapers();
        assertEquals(rows, 13);

        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/ErrorRows").toFile(), new String[]{"csv"}, false).size(), 1);
        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/Processed").toFile(), new String[]{"csv"}, false).size(), 3);
        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/Source").toFile(), new String[]{"csv"}, false).size(), 0);
    }

    @Test
    public void testKafkaProcessPapers() throws IOException {
        PapersProducer kafkaProducer = ProducerFactory.getInstance("PapersKafkaProducer", ElectionsProperties.getInstance("../resources/election.properties"));
        PaperType paperType = PaperTypeFactory.getInstance("PaperType_Presidential_2018", null);

        PapersProcessor papersProcessor = new PapersProcessor("./src/test/resources/Source", kafkaProducer, paperType, "testTopic", 0);
        int rows = papersProcessor.processPapers();
        assertEquals(rows, 13);

        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/ErrorRows").toFile(), new String[]{"csv"}, false).size(), 1);
        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/Processed").toFile(), new String[]{"csv"}, false).size(), 3);
        assertEquals(FileUtils.listFiles(Paths.get("./src/test/resources/Source").toFile(), new String[]{"csv"}, false).size(), 0);
    }


}