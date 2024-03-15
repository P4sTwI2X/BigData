import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.io.*;
import java.util.HashSet;
import java.util.Scanner;

public class RemoveStopwords {

    // public static void loadStopwords() throws IOException {
    //     stopwords = Files.readAllLines(Paths.get("stopwords.txt"));
    // }   

    public static void main(String[] args) throws IOException {
        // File paths
        Path file1Path = Paths.get("001.txt");
        Path file2Path = Paths.get("stopwords.txt");
        Path outputFilePath = Paths.get("output.txt");

        List<String> essayWords = new ArrayList<>();

        for (String line : essayLines) {
            List<String> words = Arrays.asList(line.split(" "));
            essayWords.addAll(words);
        }
        // Read file1.txt into a HashSet
        HashSet<String> wordsInFile1 = new HashSet<>(Files.lines(file1Path).collect(Collectors.toList()));
        // ArrayList<String> arraylist2 = new ArrayList<>();
        // Read file2.txt into a HashSet
        HashSet<String> stopwords = new HashSet<>(Files.lines(file2Path).collect(Collectors.toList()));
        System.out.println(wordsInFile1);
        System.out.println(stopwords);
        // Remove stopwords from wordsInFile1
        wordsInFile1.removeAll(stopwords);

        // Write the non-duplicate words to output.txt
        Files.write(outputFilePath, wordsInFile1);
        
        System.out.println("Elements removed from file1.txt. Modified content written to: " + outputFilePath);
    }
}