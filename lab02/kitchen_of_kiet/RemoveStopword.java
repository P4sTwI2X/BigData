import java.io.*;
import java.util.HashSet;
import java.util.Scanner;

public class RemoveStopword {

    public static void main(String[] args) throws IOException {
        // File paths
        String file1Path = "001.txt";
        String file2Path = "stopwords.txt";
        String outputFilePath = "sus.txt";

        File file1 = new File("001.txt");
        // Scanner scanner2 = new Scanner(file2);
        // Read elements from file2 into a HashSet for faster lookups
        HashSet<String> elementsInFile2 = new HashSet<>();
        File file2 = new File("stopwords.txt");
        Scanner scanner2 = new Scanner(file2);
        // Scanner scanner2 = new Scanner(new File(file2Path));
        while (scanner2.hasNextLine()) {
            String stopword = scanner2.nextLine();
            elementsInFile2.add(stopword);
        }
        scanner2.close();

        // Read elements from file1, write only non-duplicates to output file
        FileWriter writer = new FileWriter(outputFilePath);
        Scanner scanner1 = new Scanner(file1);
        while (scanner1.hasNextLine()) {
            String line = scanner1.nextLine().trim();
            if (!elementsInFile2.contains(line)) {
                writer.write(line + "\n");
            }
        }
        scanner1.close();
        writer.close();

        System.out.println("Elements removed from file1. Modified content written to: " + outputFilePath);
    }
}