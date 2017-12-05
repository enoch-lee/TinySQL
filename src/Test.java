import java.io.IOException;
import java.util.Scanner;

public class Test {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        Query.reset();
        Scanner in = new Scanner(System.in);
        System.out.println("Set Up Output File Name:");
        String outputFile = in.nextLine();
        Query.setFileName("src/" + outputFile);
        while (true) {
            System.out.println("Select TinySQL Mode (Press 1 or 2 to Select and Press Q to quit):");
            System.out.println("1.Input SQL Statement");
            System.out.println("2.Input SQL File Name");
            String s = in.nextLine();
            if(s.equalsIgnoreCase("1")) {
                System.out.println("Please Input SQL Query at a Line:");
                String sql = in.nextLine();
                if(sql.equalsIgnoreCase("q")) break;
                Query.parseQuery(sql);
            } else if (s.equalsIgnoreCase("2")) {
                System.out.println("Please Input a File Name:");
                String fileName = in.nextLine();
                if(fileName.equalsIgnoreCase("q")) break;
                Query.readFile("src/" + fileName);
            }else if(s.equalsIgnoreCase("Q")){
                break;
            }else{
                System.out.println("Invalid Input!");
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Elapsed: " + (endTime - startTime) / 1000 + "s");
    }
}
