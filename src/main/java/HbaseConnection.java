import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

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
            Sort.SortByValue sorter = new Sort.SortByValue(filePath);
            List<Map.Entry<String, Integer>> sortedList = sorter.getSortedList();

            // 5. 插入排序后的结果
            String upsertSQL = "UPSERT INTO ProductKeySortedCount (Product, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            // =============== 录入 ===================

            int limit = 100;
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
            System.out.println("=== 商品关键字统计数据 ===");
            while (rs.next()) {
                System.out.println(rs.getString("Product") + "\t" + rs.getInt("Count"));
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (stmt != null) stmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (connection != null) connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    // 将行为记录储存起来
    public void StoreActionStatistics(String filePath) {

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
            String createSQL = "CREATE TABLE IF NOT EXISTS ActionStatistics (" +
                    "Action VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)";
            stmt.executeUpdate(createSQL);

            // 3. 清空表数据（避免重复插入）
            stmt.executeUpdate("DELETE FROM ActionStatistics");

            // 4. 使用 SortByValue 读取并排序
            Sort.SortByValue sorter = new Sort.SortByValue(filePath);
            List<Map.Entry<String, Integer>> sortedList = sorter.getSortedList();

            // 5. 插入排序后的结果
            String upsertSQL = "UPSERT INTO ActionStatistics (Action, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            // =============== 录入 ===================

            for (Map.Entry<String, Integer> entry : sortedList) {

                String action = entry.getKey();

                // 通过条件才入库
                pstmt.setString(1, action);
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
            }

            // 6. 提交事务（Phoenix 默认 autoCommit=false）
            connection.commit();

            // 7. 查询验证
            ResultSet rs = stmt.executeQuery("SELECT * FROM ActionStatistics ORDER BY Count DESC, Action ASC");
            System.out.println("=== 行为统计数据 ===");
            while (rs.next()) {
                System.out.println(rs.getString("Action") + "\t" + rs.getInt("Count"));
            }
            rs.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (stmt != null) stmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (connection != null) connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    // 将时间记录储存起来
    public void StoreUserDailyActivity(String filePath) {

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
            String createSQL = "CREATE TABLE IF NOT EXISTS UserDailyActivity (" +
                    "Hour VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)";
            stmt.executeUpdate(createSQL);

            // 3. 清空表数据（避免重复插入）
            stmt.executeUpdate("DELETE FROM UserDailyActivity");

            // 4. 使用 SortByValue 读取并排序
            Sort.SortByKey sorter = new Sort.SortByKey(filePath);
            List<Map.Entry<String, Integer>> sortedList = sorter.getSortedList();

            // 5. 插入排序后的结果
            String upsertSQL = "UPSERT INTO UserDailyActivity (Hour, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            // =============== 录入 ===================

            for (Map.Entry<String, Integer> entry : sortedList) {

                String action = entry.getKey();

                // 通过条件才入库
                pstmt.setString(1, action);
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
            }

            // 6. 提交事务（Phoenix 默认 autoCommit=false）
            connection.commit();

            // 7. 查询验证
            ResultSet rs = stmt.executeQuery("SELECT * FROM UserDailyActivity ORDER BY Hour ASC, Count DESC");

            System.out.println("=== 用户日活跃度柱状图 ===");

            // 先把结果存到存放键值对的 List 里
            List<Map.Entry<String, Integer>> results = new ArrayList<>();
            int maxCount = 0;

            while (rs.next()) {
                String hour = rs.getString("Hour");
                int count = rs.getInt("Count");
                results.add(new AbstractMap.SimpleEntry<>(hour, count));

                // 找到最大值，方便确定比例
                if (count > maxCount) {
                    maxCount = count;
                }
            }
            rs.close();

            // 再次遍历，按比例打印柱状图
            for (Map.Entry<String, Integer> entry : results) {
                String hour = entry.getKey();
                int count = entry.getValue();

                // 按比例缩放，最大长度 50
                int barLength = (int) ((count / (double) maxCount) * 50);

                StringBuilder bar = new StringBuilder();
                for (int i = 0; i < barLength; i++) {
                    bar.append("█");
                }

                System.out.printf("%-5s | %-5d %s%n", hour, count, bar.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) pstmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (stmt != null) stmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (connection != null) connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    // 将商品转化率结果储存起来
    public void StoreItemConversion(String filePath) {

        String url = "jdbc:phoenix:node1:2181";

        Connection connection = null;
        PreparedStatement collectStmt = null;
        PreparedStatement payStmt = null;
        Statement stmt = null;

        try {
            // 1. 建立连接
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            // 2. 创建两张表：收藏率 TOP10、支付转化率 TOP10
            String createCollectSQL = "CREATE TABLE IF NOT EXISTS ItemCollectRateTop10 (" +
                    "ItemId VARCHAR PRIMARY KEY, " +
                    "ClickCount INTEGER, " +
                    "CollectCount INTEGER, " +
                    "CartCount INTEGER, " +
                    "CollectRate DOUBLE)";
            stmt.executeUpdate(createCollectSQL);

            String createPaySQL = "CREATE TABLE IF NOT EXISTS ItemPayRateTop10 (" +
                    "ItemId VARCHAR, " +
                    "PayType VARCHAR, " +
                    "ClickCount INTEGER, " +
                    "PayCount INTEGER, " +
                    "PayRate DOUBLE, " +
                    "CONSTRAINT PK_ItemPay PRIMARY KEY (ItemId, PayType))";
            stmt.executeUpdate(createPaySQL);

            // 3. 清空旧数据，避免重复
            stmt.executeUpdate("DELETE FROM ItemCollectRateTop10");
            stmt.executeUpdate("DELETE FROM ItemPayRateTop10");

            // 4. 从 MR 输出读取每个商品的统计
            List<CollectRecord> collectRecords = new ArrayList<>();
            List<PayRecord> payRecords = new ArrayList<>();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(filePath)), "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] parts = line.split("\t");
                    if (parts.length < 5) {
                        continue;
                    }

                    String itemId = parts[0];

                    int clickCount;
                    int collectCount;
                    int cartCount;
                    int alipayCount;

                    try {
                        clickCount = Integer.parseInt(parts[1]);
                        collectCount = Integer.parseInt(parts[2]);
                        cartCount = Integer.parseInt(parts[3]);
                        alipayCount = Integer.parseInt(parts[4]);
                    } catch (NumberFormatException e) {
                        continue;
                    }

                    if (clickCount <= 0) {
                        // 没有点击，无法计算转化率
                        continue;
                    }

                    double collectRate = collectCount / (double) clickCount;
                    collectRecords.add(new CollectRecord(itemId, clickCount, collectCount, cartCount, collectRate));

                    if (alipayCount > 0) {
                        double payRate = alipayCount / (double) clickCount;
                        payRecords.add(new PayRecord(itemId, "ALIPAY", clickCount, alipayCount, payRate));
                    }
                }
            }

            // 5. 排序并截取前 10
            collectRecords.sort((a, b) -> {
                int cmp = Double.compare(b.collectRate, a.collectRate);
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(b.collectCount, a.collectCount);
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(b.cartCount, a.cartCount);
                if (cmp != 0) {
                    return cmp;
                }
                return a.itemId.compareTo(b.itemId);
            });

            payRecords.sort((a, b) -> {
                int cmp = Double.compare(b.payRate, a.payRate);
                if (cmp != 0) {
                    return cmp;
                }
                cmp = Integer.compare(b.payCount, a.payCount);
                if (cmp != 0) {
                    return cmp;
                }
                return a.itemId.compareTo(b.itemId);
            });

            // 6. 插入 TOP10
            String upsertCollect = "UPSERT INTO ItemCollectRateTop10 (ItemId, ClickCount, CollectCount, CartCount, CollectRate) VALUES (?, ?, ?, ?, ?)";
            collectStmt = connection.prepareStatement(upsertCollect);

            // 使用 collectRecords 记录前 i 个商品，循环录入
            int collectLimit = Math.min(10, collectRecords.size());
            for (int i = 0; i < collectLimit; i++) {
                CollectRecord record = collectRecords.get(i);
                collectStmt.setString(1, record.itemId);
                collectStmt.setInt(2, record.clickCount);
                collectStmt.setInt(3, record.collectCount);
                collectStmt.setInt(4, record.cartCount);
                collectStmt.setDouble(5, record.collectRate);
                collectStmt.executeUpdate();
            }

            String upsertPay = "UPSERT INTO ItemPayRateTop10 (ItemId, PayType, ClickCount, PayCount, PayRate) VALUES (?, ?, ?, ?, ?)";
            payStmt = connection.prepareStatement(upsertPay);

            // 使用 payRecords 记录前 i 个商品，循环录入
            int payLimit = Math.min(10, payRecords.size());
            for (int i = 0; i < payLimit; i++) {
                PayRecord record = payRecords.get(i);
                payStmt.setString(1, record.itemId);
                payStmt.setString(2, record.payType);
                payStmt.setInt(3, record.clickCount);
                payStmt.setInt(4, record.payCount);
                payStmt.setDouble(5, record.payRate);
                payStmt.executeUpdate();
            }

            // 7. 提交事务
            connection.commit();

            // 8. 打印结果
            System.out.println("=== 商品收藏率 TOP10 ===");
            for (int i = 0; i < collectLimit; i++) {
                CollectRecord record = collectRecords.get(i);
                System.out.printf(Locale.CHINA, "%d. 商品: %s 点击: %d 收藏: %d 加购: %d 收藏率: %.4f%n",
                        i + 1, record.itemId, record.clickCount, record.collectCount, record.cartCount, record.collectRate);
            }

            System.out.println("=== 商品购买率 TOP10（按支付方式） ===");
            for (int i = 0; i < payLimit; i++) {
                PayRecord record = payRecords.get(i);
                System.out.printf(Locale.CHINA, "%d. 商品: %s 支付方式: %s 点击: %d 购买: %d 购买率: %.4f%n",
                        i + 1, record.itemId, record.payType, record.clickCount, record.payCount, record.payRate);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (collectStmt != null) collectStmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (payStmt != null) payStmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (stmt != null) stmt.close();
            } catch (Exception ignored) {
            }
            try {
                if (connection != null) connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    // 收藏率记录
    private static class CollectRecord {
        private final String itemId;
        private final int clickCount;
        private final int collectCount;
        private final int cartCount;
        private final double collectRate;

        private CollectRecord(String itemId, int clickCount, int collectCount, int cartCount, double collectRate) {
            this.itemId = itemId;
            this.clickCount = clickCount;
            this.collectCount = collectCount;
            this.cartCount = cartCount;
            this.collectRate = collectRate;
        }
    }

    // 支付率记录
    private static class PayRecord {
        private final String itemId;
        private final String payType;
        private final int clickCount;
        private final int payCount;
        private final double payRate;

        private PayRecord(String itemId, String payType, int clickCount, int payCount, double payRate) {
            this.itemId = itemId;
            this.payType = payType;
            this.clickCount = clickCount;
            this.payCount = payCount;
            this.payRate = payRate;
        }
    }
}

