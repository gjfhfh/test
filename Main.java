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

    int degA = in.nextInt();
    int nA = degA + 1;
    double[] polyA = new double[nA];
    for (int i = degA; i >= 0; i--) {
      double coeff = in.nextDouble();
      polyA[i] = coeff;
    }

    int degB = in.nextInt();
    int nB = degB + 1;
    double[] polyB = new double[nB];
    for (int i = degB; i >= 0; i--) {
      polyB[i] = in.nextInt();
    }

    long[] result = multiply(polyA, polyB);
    int degC = result.length - 1;
    out.print(degC);
    for (int i = degC; i >= 0; i--) {
      out.print(' ');
      out.print(result[i]);
    }
    out.println();
    out.flush();
  }

  private static long[] multiply(double[] a, double[] b) {
    int n = 1;
    int resultLength = a.length + b.length - 1;
    while (n < resultLength) {
      n <<= 1;
    }

    double[] faRe = Arrays.copyOf(a, n);
    double[] faIm = new double[n];
    double[] fbRe = Arrays.copyOf(b, n);
    double[] fbIm = new double[n];

    fft(faRe, faIm, false);
    fft(fbRe, fbIm, false);

    for (int i = 0; i < n; i++) {
      double real = faRe[i] * fbRe[i] - faIm[i] * fbIm[i];
      double imag = faRe[i] * fbIm[i] + faIm[i] * fbRe[i];
      faRe[i] = real;
      faIm[i] = imag;
    }

    fft(faRe, faIm, true);

    long[] res = new long[resultLength];
    for (int i = 0; i < resultLength; i++) {
      res[i] = Math.round(faRe[i]);
    }
    return res;
  }

  private static void fft(double[] re, double[] im, boolean invert) {
    int n = re.length;
    int j = 0;
    for (int i = 1; i < n; i++) {
      int bit = n >> 1;
      while ((j & bit) != 0) {
        j ^= bit;
        bit >>= 1;
      }
      j ^= bit;
      if (i < j) {
        double tempRe = re[i];
        double tempIm = im[i];
        re[i] = re[j];
        im[i] = im[j];
        re[j] = tempRe;
        im[j] = tempIm;
      }
    }

    for (int len = 2; len <= n; len <<= 1) {
      double angle = 2 * Math.PI / len * (invert ? -1 : 1);
      double wLenRe = Math.cos(angle);
      double wLenIm = Math.sin(angle);
      for (int i = 0; i < n; i += len) {
        double wRe = 1.0;
        double wIm = 0.0;
        for (int k = 0; k < len / 2; k++) {
          int u = i + k;
          int v = i + k + len / 2;

          double tRe = wRe * re[v] - wIm * im[v];
          double tIm = wRe * im[v] + wIm * re[v];

          re[v] = re[u] - tRe;
          im[v] = im[u] - tIm;
          re[u] += tRe;
          im[u] += tIm;

          double nextWRe = wRe * wLenRe - wIm * wLenIm;
          double nextWIm = wRe * wLenIm + wIm * wLenRe;
          wRe = nextWRe;
          wIm = nextWIm;
        }
      }
    }

    if (invert) {
      for (int i = 0; i < n; i++) {
        re[i] /= n;
        im[i] /= n;
      }
    }
  }
}
