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
                    String line = reader.readLine();
                    if (line == null) {
                        return null;
                    }
                    tokenizer = new StringTokenizer(line);
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
                if (tokenizer != null && tokenizer.hasMoreTokens()) {
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

    private static final double EPS = 1e-9;

    private static class Point {
        double x, y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }

    public static void main(String[] args) throws IOException {
        FastReader in = new FastReader();
        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

        String first = in.next();
        if (first == null) {
            out.flush();
            return;
        }
        double width = Double.parseDouble(first);
        double height = in.nextDouble();
        int n = in.nextInt();

        Point[] players = new Point[n];
        for (int i = 0; i < n; i++) {
            double px = in.nextDouble();
            double py = in.nextDouble();
            players[i] = new Point(px, py);
        }

        List<Point> field = Arrays.asList(
                new Point(0.0, 0.0),
                new Point(width, 0.0),
                new Point(width, height),
                new Point(0.0, height)
        );

        for (int i = 0; i < n; i++) {
            List<Point> cell = new ArrayList<>(field);
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                Point pi = players[i];
                Point pj = players[j];
                double A = 2.0 * (pj.x - pi.x);
                double B = 2.0 * (pj.y - pi.y);
                double C = pi.x * pi.x - pj.x * pj.x + pi.y * pi.y - pj.y * pj.y;
                cell = clipWithHalfPlane(cell, A, B, C);
                if (cell.isEmpty()) {
                    break;
                }
            }

            cell = cleanPolygon(cell);
            cell = normalizePolygon(cell);

            out.print(cell.size());
            for (Point p : cell) {
                out.printf(Locale.US, " %.8f %.8f", p.x, p.y);
            }
            if (i + 1 < n) {
                out.println();
            }
        }

        out.flush();
    }

    private static List<Point> clipWithHalfPlane(List<Point> poly, double A, double B, double C) {
        List<Point> res = new ArrayList<>();
        if (poly.isEmpty()) {
            return res;
        }
        int m = poly.size();
        for (int i = 0; i < m; i++) {
            Point cur = poly.get(i);
            Point next = poly.get((i + 1) % m);
            double valCur = A * cur.x + B * cur.y + C;
            double valNext = A * next.x + B * next.y + C;
            boolean inCur = valCur <= EPS;
            boolean inNext = valNext <= EPS;

            if (inCur && inNext) {
                res.add(next);
            } else if (inCur && !inNext) {
                res.add(intersection(cur, next, A, B, C));
            } else if (!inCur && inNext) {
                res.add(intersection(cur, next, A, B, C));
                res.add(next);
            }
        }
        return res;
    }

    private static Point intersection(Point a, Point b, double A, double B, double C) {
        double valA = A * a.x + B * a.y + C;
        double valB = A * b.x + B * b.y + C;
        double t = valA / (valA - valB);
        double x = a.x + (b.x - a.x) * t;
        double y = a.y + (b.y - a.y) * t;
        return new Point(x, y);
    }

    private static List<Point> cleanPolygon(List<Point> poly) {
        if (poly.isEmpty()) {
            return poly;
        }
        List<Point> res = new ArrayList<>();
        for (Point p : poly) {
            if (!res.isEmpty()) {
                Point last = res.get(res.size() - 1);
                if (Math.hypot(p.x - last.x, p.y - last.y) <= EPS) {
                    continue;
                }
            }
            res.add(p);
        }
        if (res.size() > 1) {
            Point first = res.get(0);
            Point last = res.get(res.size() - 1);
            if (Math.hypot(first.x - last.x, first.y - last.y) <= EPS) {
                res.remove(res.size() - 1);
            }
        }

        boolean changed;
        do {
            changed = false;
            int m = res.size();
            if (m < 3) break;
            for (int i = 0; i < m; i++) {
                Point prev = res.get((i + m - 1) % m);
                Point cur = res.get(i);
                Point next = res.get((i + 1) % m);
                double cross = (cur.x - prev.x) * (next.y - cur.y) - (cur.y - prev.y) * (next.x - cur.x);
                if (Math.abs(cross) <= EPS) {
                    res.remove(i);
                    changed = true;
                    break;
                }
            }
        } while (changed);
        return res;
    }

    private static List<Point> normalizePolygon(List<Point> poly) {
        if (poly.isEmpty()) {
            return poly;
        }
        double area = 0.0;
        int m = poly.size();
        for (int i = 0; i < m; i++) {
            Point a = poly.get(i);
            Point b = poly.get((i + 1) % m);
            area += a.x * b.y - b.x * a.y;
        }
        if (area < 0) {
            Collections.reverse(poly);
        }

        int start = 0;
        for (int i = 1; i < poly.size(); i++) {
            Point p = poly.get(i);
            Point s = poly.get(start);
            if (p.y < s.y - EPS || (Math.abs(p.y - s.y) <= EPS && p.x < s.x - EPS)) {
                start = i;
            }
        }

        List<Point> res = new ArrayList<>(poly.size());
        for (int i = 0; i < poly.size(); i++) {
            res.add(poly.get((start + i) % poly.size()));
        }
        return res;
    }
}
