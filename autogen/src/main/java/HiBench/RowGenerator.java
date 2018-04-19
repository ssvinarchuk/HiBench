package HiBench;

import org.apache.hadoop.fs.shell.PathData;

import java.io.*;
import java.util.Random;

public class RowGenerator {

    public static String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    public static String ENCODING = "UTF-8";

    private static int charAmount;
    private static int repeat;
    private static int keyCount;
    private static String path;

    public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
        parseArgs(args);


        try(PrintWriter pw = new PrintWriter(path, ENCODING)) {

            for(int i = 0; i < keyCount; i++) {
                String row = i + "," + generateRandomRow();
                pw.println(row);
            }
        }
    }

    private static String generateRandomRow() {
        StringBuilder builder = new StringBuilder();
        Random rnd = new Random();

        while (builder.length() < charAmount) {
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            builder.append(SALTCHARS.charAt(index));
        }
        String result = "";

        for(int i = 0; i < repeat; i++) {
            result = result + builder.toString();
        }

        return result;
    }

    private static void parseArgs(String args[]) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-kn")) {
                keyCount = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-r")) {
                repeat = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-c")) {
                charAmount = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-p")) {
                path = args[++i];
            }
        }
    }
}
