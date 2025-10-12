import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HbaseConnection {

    // 将词频排序结果储存起来
    public void StoreKeywordCount(String filePath) {

        // JDBC URL
        String url = "jdbc:phoenix:node1:2181";

        Connection connection = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        try {
            // 1. 建立连接
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            // 2. 创建表（如果不存在）
            String createSQL = "CREATE TABLE IF NOT EXISTS ProductKeySortedCount (" +
                    "Product VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)";
            stmt.executeUpdate(createSQL);

            // 3. 清空表数据（避免重复插入）
            stmt.executeUpdate("DELETE FROM ProductKeySortedCount");

            // 4. 使用 SortByValue 读取并排序
            SortByValue sorter = new SortByValue(filePath);
            List<Map.Entry<String, Integer>> sortedList = sorter.getSortedList();

            // 5. 插入排序后的结果
            String upsertSQL = "UPSERT INTO ProductKeySortedCount (Product, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            // =============== 录入 ===================

            int limit = 1000;
            int count = 0;

            for (Map.Entry<String, Integer> entry : sortedList) {
                if (count >= limit) {
                    break;
                }

                String product = entry.getKey();

                // 过滤掉非法情况
                if (!product.matches("[a-zA-Z0-9\\u4e00-\\u9fa5]+")) {
                    continue; // 含有符号，跳过
                }
                if (product.length() < 2) {
                    continue; // 单个字符，跳过
                }
                if (product.matches("\\d+")) {
                    continue; // 纯数字，跳过
                }

                // 通过条件才入库
                pstmt.setString(1, product);
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
                count++;
            }

            // 6. 提交事务（Phoenix 默认 autoCommit=false）
            connection.commit();

            // 7. 查询验证
            ResultSet rs = stmt.executeQuery("SELECT * FROM ProductKeySortedCount ORDER BY Count DESC, Product ASC");
            System.out.println("=== Phoenix Table Data ===");
            while (rs.next()) {
                System.out.println(rs.getString("Product") + "\t" + rs.getInt("Count"));
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (pstmt != null) pstmt.close(); } catch (Exception ignored) {}
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }
}
