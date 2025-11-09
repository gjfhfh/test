import java.io.*;
import java.util.*;

public class Main {
    private static class FastReader {

        BufferedReader reader;
        StringTokenizer tokenizer;

        private FastReader() {
            reader = new BufferedReader(new InputStreamReader(System.in));
        }

        private String next() {
            while (tokenizer == null || !tokenizer.hasMoreElements()) {
                try {
                    tokenizer = new StringTokenizer(reader.readLine());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return tokenizer.nextToken();
        }

        private int nextInt() {
            return Integer.parseInt(next());
        }

        private long nextLong() {
            return Long.parseLong(next());
        }

        private double nextDouble() {
            return Double.parseDouble(next());
        }

        private String nextLine() {
            String str = "";
            try {
                if (tokenizer.hasMoreTokens()) {
                    str = tokenizer.nextToken("\n");
                } else {
                    str = reader.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return str;
        }
    }


    public static void main(String[] args) {
        FastReader in = new FastReader();
        PrintWriter out = new PrintWriter(System.out);

        out.flush();
    }
}
