import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Sort {

    // 文件读取抽象类
    public abstract static class BaseSorter {

        // 创建临时储存键值对的 List
        protected List<Map.Entry<String, Integer>> sortedList;

        public BaseSorter(String filePath) {
            sortedList = new ArrayList<>();
            readFile(filePath);
        }

        // 读取文件内容
        private void readFile(String filePath) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length == 2) {
                        String key = parts[0];
                        int value = Integer.parseInt(parts[1]);
                        sortedList.add(new AbstractMap.SimpleEntry<>(key, value));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 抽象子类实现具体的排序逻辑
        protected abstract Comparator<Map.Entry<String, Integer>> getComparator();

        // 排序方法
        protected void performSort() {
            sortedList.sort(getComparator());
        }

        // 获取排序结果
        public List<Map.Entry<String, Integer>> getSortedList() {
            return sortedList;
        }

        // 打印排序结果
        public void print() {
            for (Map.Entry<String, Integer> entry : sortedList) {
                System.out.println(entry.getKey() + "\t" + entry.getValue());
            }
        }
    }

    public static class SortByValue extends BaseSorter {

        public SortByValue(String filePath) {
            super(filePath);
            performSort();
        }

        // 覆写 getComparator()，根据 Value 降序排列
        @Override
        protected Comparator<Map.Entry<String, Integer>> getComparator() {
            return (e1, e2) -> {
                int cmp = e2.getValue().compareTo(e1.getValue()); // value 倒序
                return (cmp != 0) ? cmp : e1.getKey().compareTo(e2.getKey()); // key 升序
            };
        }
    }

    public static class SortByKey extends BaseSorter {

        public SortByKey(String filePath) {
            super(filePath);
            performSort();
        }

        // 覆写 getComparator()，根据 Key 降升序排列
        @Override
        protected Comparator<Map.Entry<String, Integer>> getComparator() {
            return (e1, e2) -> {
                int cmp = e1.getKey().compareTo(e2.getKey()); // key 升序
                return (cmp != 0) ? cmp : e2.getValue().compareTo(e1.getValue()); // value 倒序
            };
        }
    }
}