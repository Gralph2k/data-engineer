package ru.gralph2k.de;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.testng.Assert.*;

public class FileHelperTest {
    FileHelper fileHelper;

    @BeforeMethod
    public void setUp() throws IOException {
        fileHelper = FileHelper.getInstance("./src/test/resources/Source",null);
        FileUtils.forceMkdir(Paths.get("./src/test/resources/Source").toFile());
        FileUtils.cleanDirectory(Paths.get("./src/test/resources/Source").toFile());
        FileUtils.copyDirectory(Paths.get("./src/test/resources/Template/").toFile(), Paths.get("./src/test/resources/Source/").toFile());
    }

    @Test
    public void testFileList() {
        File[] files = fileHelper.papersList();
        assertEquals(files.length,3);
        assertEquals(files[0].toPath().getFileName().toString(),"1.csv");
    }
}