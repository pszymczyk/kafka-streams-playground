package com.pszymczyk.common;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Utils {

    public static List<String> readLines(String path) {
        try {
            return Files.readAllLines(Path.of(Utils.class.getClassLoader().getResource(path).toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
