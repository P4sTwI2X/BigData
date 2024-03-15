import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.io.*;
import java.util.HashSet;
import java.util.Scanner;
import java.util.*;

public class Test {

    // public static void loadStopwords() throws IOException {
    //     stopwords = Files.readAllLines(Paths.get("stopwords.txt"));
    // }   

    public static void main(String[] args) throws IOException {
        // File paths
        Path file1Path = Paths.get("001.txt");
        Path file2Path = Paths.get("stopwords.txt");
        Path outputFilePath = Paths.get("output.txt");

        Scanner s = new Scanner(new File("001.txt"));
        ArrayList<String> list = new ArrayList<String>();
        while (s.hasNext()){
            list.add(s.next().toLowerCase());
        }
        s.close();

        Scanner stopword = new Scanner(new File("stopwords.txt"));
        ArrayList<String> stoplist = new ArrayList<String>();
        while (stopword.hasNext()){
            stoplist.add(stopword.next().toLowerCase());
        }
        stopword.close();
    
        list.removeAll(stoplist);
        Files.write(outputFilePath, list);
    }
}