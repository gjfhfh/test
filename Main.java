package org.example;

import java.util.*;

public class Main {
  public static void main(String[] args) {
    Scanner scanner = new Scanner(System.in);
    int q = scanner.nextInt();
    scanner.nextLine();
    MinHeap heap = new MinHeap();

    for (int i = 0; i < q; i++) {
      String[] command = scanner.nextLine().split(" ");

      if (command[0].equals("insert")) {
        int x = Integer.parseInt(command[1]);
        heap.insert(x, i + 1);
      } else if (command[0].equals("getMin")) {
        System.out.println(heap.getMin());
      } else if (command[0].equals("extractMin")) {
        heap.extractMin();
      } else if (command[0].equals("decreaseKey")) {
        heap.decreaseKey(Integer.parseInt(command[1]), Integer.parseInt(command[2]));
      }
    }
    scanner.close();
  }

  static class MinHeap {
    private List<Integer> heap = new ArrayList<>();
    private Map<Integer, Integer> requests = new HashMap<Integer, Integer>();

    public void insert(int x, int requestNum) {
      heap.add(x);
      requests.put(requestNum, x);
      siftUp(heap.size() - 1);
    }

    public int getMin() {
      return heap.get(0);
    }

    public void extractMin() {
      if (heap.size() == 1) {
        heap.remove(0);
      } else {
        heap.set(0, heap.remove(heap.size() - 1));
        siftDown(0);
      }
    }

    public void decreaseKey(int requestNum, int delta) {
      int oldValue = requests.get(requestNum);
      int newValue = oldValue - delta;

      int position = -1;
      for (int i = 0; i < heap.size(); i++) {
        if (heap.get(i) == oldValue) {
          position = i;
          break;
        }
      }
      if (position == -1) return;

      heap.set(position, newValue);
      requests.put(requestNum, newValue);
      siftUp(position);
    }

    private void siftUp(int i) {
      while (i > 0) {
        int parent = (i - 1) / 2;
        if (heap.get(i) >= heap.get(parent)) {
          break;
        }

        int compare = heap.get(i);
        heap.set(i, heap.get(parent));
        heap.set(parent, compare);
        i = parent;
      }
    }

    private void siftDown(int i) {
      while (true) {
        int left = 2 * i + 1;
        int right = 2 * i + 2;
        int min = i;

        if (left < heap.size() && heap.get(left) < heap.get(min)) {
          min = left;
        }
        if (right < heap.size() && heap.get(right) < heap.get(min)) {
          min = right;
        }
        if (min == i) {
          break;
        }

        int compare = heap.get(i);
        heap.set(i, heap.get(min));
        heap.set(min, compare);
        i = min;
      }
    }
  }
}
