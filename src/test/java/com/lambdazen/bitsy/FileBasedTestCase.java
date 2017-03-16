package com.lambdazen.bitsy;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import junit.framework.TestCase;

public class FileBasedTestCase extends TestCase {
    public Path tempDir(String dirName) throws IOException {
        return tempDir(dirName, true);
    }
    
    public Path tempDir(String dirName, boolean delete) throws IOException {
        String clsUri = this.getClass().getName().replace('.','/') + ".class";
        URL url = this.getClass().getClassLoader().getResource(clsUri);
        String clsPath = url.getPath();
        System.out.println(clsPath);
        String dir = clsPath.substring(clsPath.indexOf(":") + 1, clsPath.length() - clsUri.length());
        System.out.println(dir);
        Path root = Paths.get(dir).resolve("../" + dirName).normalize();
        System.out.println(root);
        
        if (!Files.exists(root)) {
            Files.createDirectory(root);
        }
        
        if (delete) {
            deleteDirectory(root.toFile(), false);
        }
        
        return root;
    }

    protected static void deleteDirectory(final File directory, boolean deleteDir) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file, true);
                } else {
                    file.delete();
                }
            }
            
            if (deleteDir) {
                directory.delete();
            }
        }
    }
    
    public void testDummy() throws IOException {
        tempDir("temp-dir");
    }
}
