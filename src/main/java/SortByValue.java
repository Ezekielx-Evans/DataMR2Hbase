import java.io.*;
import java.util.*;

// 根据词频排序类
public class SortByValue {

    private List<Map.Entry<String, Integer>> sortedList; // 保存排序后的结果

    // 构造方法：传入文件路径，自动读取并排序
    public SortByValue(String filePath) {


        sortedList = new ArrayList<>();

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

        // 排序：按 value 倒序，如果相等再按 key 升序
        sortedList.sort((e1, e2) -> {
            int cmp = e2.getValue().compareTo(e1.getValue());
            return (cmp != 0) ? cmp : e1.getKey().compareTo(e2.getKey());
        });
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
