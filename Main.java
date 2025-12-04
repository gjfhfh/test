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

    public static void main(String[] args) throws IOException {
        FastReader in = new FastReader();
        PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

        List<String> outputs = new ArrayList<>();

        while (true) {
            int n;
            try {
                n = in.nextInt();
            } catch (Exception e) {
                break;
            }

            List<Point> poly = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                double x = in.nextDouble();
                double y = in.nextDouble();
                poly.add(new Point(x, y));
            }

            if (signedArea(poly) < 0) {
                Collections.reverse(poly);
            }

            double[] bounds = boundingBox(poly);
            double diag = Math.hypot(bounds[1] - bounds[0], bounds[3] - bounds[2]);
            double low = 0.0;
            double high = diag;

            Point[] bestPair = null;
            for (int iter = 0; iter < 70; iter++) {
                double mid = (low + high) / 2.0;
                Point[] candidate = tryPlace(poly, mid);
                if (candidate != null) {
                    low = mid;
                    bestPair = candidate;
                } else {
                    high = mid;
                }
            }

            Point[] finalPair = tryPlace(poly, low);
            if (finalPair != null) {
                bestPair = finalPair;
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.US, "%.15f\n", low));
            sb.append(String.format(Locale.US, "%.15f %.15f\n", bestPair[0].x, bestPair[0].y));
            sb.append(String.format(Locale.US, "%.15f %.15f", bestPair[1].x, bestPair[1].y));
            outputs.add(sb.toString());
        }

        for (int i = 0; i < outputs.size(); i++) {
            out.print(outputs.get(i));
            if (i + 1 < outputs.size()) {
                out.print('\n');
            }
        }

        out.flush();
    }

    private static final double EPS = 1e-10;

    private static class Point {
        double x, y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        Point subtract(Point other) {
            return new Point(x - other.x, y - other.y);
        }

        Point add(Point other) {
            return new Point(x + other.x, y + other.y);
        }

        Point scale(double k) {
            return new Point(x * k, y * k);
        }
    }

    private static double cross(Point a, Point b) {
        return a.x * b.y - a.y * b.x;
    }

    private static double dot(Point a, Point b) {
        return a.x * b.x + a.y * b.y;
    }

    private static double dist2(Point a, Point b) {
        double dx = a.x - b.x;
        double dy = a.y - b.y;
        return dx * dx + dy * dy;
    }

    private static double signedArea(List<Point> poly) {
        double area = 0.0;
        int n = poly.size();
        for (int i = 0; i < n; i++) {
            Point p = poly.get(i);
            Point q = poly.get((i + 1) % n);
            area += p.x * q.y - p.y * q.x;
        }
        return area / 2.0;
    }

    private static double[] boundingBox(List<Point> poly) {
        double minX = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        for (Point p : poly) {
            minX = Math.min(minX, p.x);
            maxX = Math.max(maxX, p.x);
            minY = Math.min(minY, p.y);
            maxY = Math.max(maxY, p.y);
        }
        return new double[]{minX, maxX, minY, maxY};
    }

    private static Point[] tryPlace(List<Point> poly, double r) {
        List<Point> offset = buildOffset(poly, r);
        if (offset.size() < 2) {
            return null;
        }

        double required = 4.0 * r * r - 1e-12;
        if (offset.size() == 2) {
            if (dist2(offset.get(0), offset.get(1)) + 1e-12 >= required) {
                return new Point[]{offset.get(0), offset.get(1)};
            }
            return null;
        }

        DiameterResult res = diameter(offset);
        if (res.maxDist2 + 1e-12 >= required) {
            return new Point[]{offset.get(res.a), offset.get(res.b)};
        }
        return null;
    }

    private static List<Point> buildOffset(List<Point> poly, double r) {
        List<Point> res = new ArrayList<>(poly);
        int n = poly.size();
        for (int i = 0; i < n; i++) {
            Point p1 = poly.get(i);
            Point p2 = poly.get((i + 1) % n);
            Point edge = p2.subtract(p1);
            double len = Math.hypot(edge.x, edge.y);
            Point normal = new Point(-edge.y / len, edge.x / len);
            double d = dot(normal, p1) + r;
            res = clipWithHalfPlane(res, normal, d);
            if (res.isEmpty()) {
                break;
            }
        }
        return res;
    }

    private static List<Point> clipWithHalfPlane(List<Point> poly, Point n, double d) {
        List<Point> res = new ArrayList<>();
        if (poly.isEmpty()) {
            return res;
        }
        int m = poly.size();
        for (int i = 0; i < m; i++) {
            Point cur = poly.get(i);
            Point next = poly.get((i + 1) % m);
            double valCur = dot(n, cur) - d;
            double valNext = dot(n, next) - d;
            boolean inCur = valCur >= -EPS;
            boolean inNext = valNext >= -EPS;

            if (inCur && inNext) {
                res.add(next);
            } else if (inCur && !inNext) {
                res.add(intersection(cur, next, n, d));
            } else if (!inCur && inNext) {
                res.add(intersection(cur, next, n, d));
                res.add(next);
            }
        }
        return res;
    }

    private static Point intersection(Point a, Point b, Point n, double d) {
        double da = dot(n, a) - d;
        double db = dot(n, b) - d;
        double t = da / (da - db);
        Point dir = b.subtract(a);
        return new Point(a.x + dir.x * t, a.y + dir.y * t);
    }

    private static class DiameterResult {
        double maxDist2;
        int a, b;

        DiameterResult(double maxDist2, int a, int b) {
            this.maxDist2 = maxDist2;
            this.a = a;
            this.b = b;
        }
    }

    private static DiameterResult diameter(List<Point> poly) {
        int n = poly.size();
        if (n == 1) {
            return new DiameterResult(0.0, 0, 0);
        }
        if (n == 2) {
            return new DiameterResult(dist2(poly.get(0), poly.get(1)), 0, 1);
        }

        double maxDist = 0.0;
        int bestA = 0;
        int bestB = 1;

        int j = 1;
        for (int i = 0; i < n; i++) {
            int ni = (i + 1) % n;
            while (true) {
                int nj = (j + 1) % n;
                double cross1 = Math.abs(cross(poly.get(ni).subtract(poly.get(i)), poly.get(nj).subtract(poly.get(i))));
                double cross2 = Math.abs(cross(poly.get(ni).subtract(poly.get(i)), poly.get(j).subtract(poly.get(i))));
                if (cross1 <= cross2 + EPS) {
                    break;
                }
                j = nj;
            }

            double d1 = dist2(poly.get(i), poly.get(j));
            if (d1 > maxDist) {
                maxDist = d1;
                bestA = i;
                bestB = j;
            }

            double d2 = dist2(poly.get(ni), poly.get(j));
            if (d2 > maxDist) {
                maxDist = d2;
                bestA = ni;
                bestB = j;
            }
        }

        return new DiameterResult(maxDist, bestA, bestB);
    }
}
