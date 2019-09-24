package ru.gralph2k.de;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.*;

public class FileHelperTest {
    FileHelper fileHelper;

    @BeforeMethod
    public void setUp() throws IOException {
        fileHelper = FileHelper.getInstance("./src/test/resources/Source",null);
    }

    @Test
    public void testFileList() {
        File[] files = fileHelper.papersList();
        assertEquals(files.length,3);
        assertEquals(files[0].toPath().getFileName().toString(),"1.csv");
    }
}