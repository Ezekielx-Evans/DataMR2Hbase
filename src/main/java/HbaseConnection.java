import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

public class HbaseConnection {

    // ====================== 商品关键字统计 ======================
    public void StoreKeywordCount(Configuration conf, String hdfsPath) {
        String url = "jdbc:phoenix:node1:2181";
        Connection connection = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        try {
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ProductKeySortedCount (" +
                    "Product VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)");
            stmt.executeUpdate("DELETE FROM ProductKeySortedCount");

            // === 从 HDFS 读取结果文件 ===
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath)), "UTF-8"));

            Map<String, Integer> map = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    map.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            reader.close();

            // 排序
            List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(map.entrySet());
            sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            String upsertSQL = "UPSERT INTO ProductKeySortedCount (Product, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            // 限制输出条数
            int limit = 100, count = 0;
            for (Map.Entry<String, Integer> entry : sortedList) {
                if (count >= limit) break;
                String product = entry.getKey();
                // 只保留由字母、数字、中文组成的字符串，如果包含其他字符（比如标点），就跳过
                if (!product.matches("[a-zA-Z0-9\\u4e00-\\u9fa5]+")) continue;
                // 字符串长度小于 2 的跳过（避免单字或无意义字符）
                if (product.length() < 2) continue;
                // 如果是纯数字（一个或多个数字），跳过
                if (product.matches("\\d+")) continue;
                pstmt.setString(1, product);
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
                count++;
            }
            connection.commit();

            ResultSet rs = stmt.executeQuery("SELECT * FROM ProductKeySortedCount ORDER BY Count DESC LIMIT 50");
            System.out.println("=== 商品关键字统计数据 ===");
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

    // ====================== 用户行为统计 ======================
    public void StoreActionStatistics(Configuration conf, String hdfsPath) {
        String url = "jdbc:phoenix:node1:2181";
        Connection connection = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        try {
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ActionStatistics (" +
                    "Action VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)");
            stmt.executeUpdate("DELETE FROM ActionStatistics");

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath)), "UTF-8"));

            Map<String, Integer> map = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    map.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            reader.close();

            List<Map.Entry<String, Integer>> sortedList = new ArrayList<>(map.entrySet());
            sortedList.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            String upsertSQL = "UPSERT INTO ActionStatistics (Action, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            for (Map.Entry<String, Integer> entry : sortedList) {
                pstmt.setString(1, entry.getKey());
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
            }
            connection.commit();

            ResultSet rs = stmt.executeQuery("SELECT * FROM ActionStatistics ORDER BY Count DESC LIMIT 10");
            System.out.println("=== 用户行为统计数据 ===");
            while (rs.next()) {
                System.out.println(rs.getString("Action") + "\t" + rs.getInt("Count"));
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

    // ====================== 用户日活统计 ======================
    public void StoreUserDailyActivity(Configuration conf, String hdfsPath) {
        String url = "jdbc:phoenix:node1:2181";
        Connection connection = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;

        try {
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS UserDailyActivity (" +
                    "Hour VARCHAR PRIMARY KEY, " +
                    "Count INTEGER)");
            stmt.executeUpdate("DELETE FROM UserDailyActivity");

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath)), "UTF-8"));

            Map<String, Integer> map = new TreeMap<>(); // 保证小时有序
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    map.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            reader.close();

            String upsertSQL = "UPSERT INTO UserDailyActivity (Hour, Count) VALUES (?, ?)";
            pstmt = connection.prepareStatement(upsertSQL);

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                pstmt.setString(1, entry.getKey());
                pstmt.setInt(2, entry.getValue());
                pstmt.executeUpdate();
            }
            connection.commit();

            ResultSet rs = stmt.executeQuery("SELECT * FROM UserDailyActivity ORDER BY Hour ASC");
            System.out.println("=== 用户日活跃度柱状图 ===");

            List<Map.Entry<String, Integer>> results = new ArrayList<>();
            int maxCount = 0;

            // 读取数据同时找最值确定图比例
            while (rs.next()) {
                String hour = rs.getString("Hour");
                int count = rs.getInt("Count");
                results.add(new AbstractMap.SimpleEntry<>(hour, count));
                if (count > maxCount) maxCount = count;
            }
            rs.close();

            // 打印柱状图
            for (Map.Entry<String, Integer> entry : results) {
                int barLength = (int) ((entry.getValue() / (double) maxCount) * 50);
                StringBuilder bar = new StringBuilder();
                for (int i = 0; i < barLength; i++) bar.append("█");
                System.out.printf("%-5s | %-5d %s%n", entry.getKey(), entry.getValue(), bar.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (pstmt != null) pstmt.close(); } catch (Exception ignored) {}
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }

    // ====================== 商品转化率统计 ======================
    public void StoreItemConversion(Configuration conf, String hdfsPath) {
        String url = "jdbc:phoenix:node1:2181";
        Connection connection = null;
        PreparedStatement collectStmt = null;
        PreparedStatement payStmt = null;
        Statement stmt = null;

        try {
            Properties properties = new Properties();
            connection = DriverManager.getConnection(url, properties);
            stmt = connection.createStatement();

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ItemCollectRateTop10 (" +
                    "ItemId VARCHAR PRIMARY KEY, " +
                    "ClickCount INTEGER, " +
                    "CollectCount INTEGER, " +
                    "CartCount INTEGER, " +
                    "CollectRate DOUBLE)");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ItemPayRateTop10 (" +
                    "ItemId VARCHAR, " +
                    "PayType VARCHAR, " +
                    "ClickCount INTEGER, " +
                    "PayCount INTEGER, " +
                    "PayRate DOUBLE, " +
                    "CONSTRAINT PK_ItemPay PRIMARY KEY (ItemId, PayType))");

            stmt.executeUpdate("DELETE FROM ItemCollectRateTop10");
            stmt.executeUpdate("DELETE FROM ItemPayRateTop10");

            List<CollectRecord> collectRecords = new ArrayList<>();
            List<PayRecord> payRecords = new ArrayList<>();

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath)), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] parts = line.split("\t");
                if (parts.length < 5) continue;

                String itemId = parts[0];
                int clickCount, collectCount, cartCount, alipayCount;
                try {
                    clickCount = Integer.parseInt(parts[1]);
                    collectCount = Integer.parseInt(parts[2]);
                    cartCount = Integer.parseInt(parts[3]);
                    alipayCount = Integer.parseInt(parts[4]);
                } catch (NumberFormatException e) {
                    continue;
                }
                if (clickCount <= 0) continue;

                // 计算收藏率
                double collectRate = collectCount / (double) clickCount;
                collectRecords.add(new CollectRecord(itemId, clickCount, collectCount, cartCount, collectRate));

                if (alipayCount > 0) {
                    double payRate = alipayCount / (double) clickCount;
                    payRecords.add(new PayRecord(itemId, "ALIPAY", clickCount, alipayCount, payRate));
                }
            }
            reader.close();

            // 计算支付率
            collectRecords.sort((a, b) -> Double.compare(b.collectRate, a.collectRate));
            payRecords.sort((a, b) -> Double.compare(b.payRate, a.payRate));

            String upsertCollect = "UPSERT INTO ItemCollectRateTop10 (ItemId, ClickCount, CollectCount, CartCount, CollectRate) VALUES (?, ?, ?, ?, ?)";
            collectStmt = connection.prepareStatement(upsertCollect);
            for (int i = 0; i < Math.min(10, collectRecords.size()); i++) {
                CollectRecord r = collectRecords.get(i);
                collectStmt.setString(1, r.itemId);
                collectStmt.setInt(2, r.clickCount);
                collectStmt.setInt(3, r.collectCount);
                collectStmt.setInt(4, r.cartCount);
                collectStmt.setDouble(5, r.collectRate);
                collectStmt.executeUpdate();
            }

            String upsertPay = "UPSERT INTO ItemPayRateTop10 (ItemId, PayType, ClickCount, PayCount, PayRate) VALUES (?, ?, ?, ?, ?)";
            payStmt = connection.prepareStatement(upsertPay);
            for (int i = 0; i < Math.min(10, payRecords.size()); i++) {
                PayRecord r = payRecords.get(i);
                payStmt.setString(1, r.itemId);
                payStmt.setString(2, r.payType);
                payStmt.setInt(3, r.clickCount);
                payStmt.setInt(4, r.payCount);
                payStmt.setDouble(5, r.payRate);
                payStmt.executeUpdate();
            }
            connection.commit();

            System.out.println("=== 商品收藏率 TOP10 ===");
            for (int i = 0; i < Math.min(10, collectRecords.size()); i++) {
                CollectRecord r = collectRecords.get(i);
                System.out.printf(Locale.CHINA, "%d. 商品:%s 点击:%d 收藏:%d 加购:%d 收藏率:%.4f%n",
                        i + 1, r.itemId, r.clickCount, r.collectCount, r.cartCount, r.collectRate);
            }
            System.out.println("=== 商品支付率 TOP10 ===");
            for (int i = 0; i < Math.min(10, payRecords.size()); i++) {
                PayRecord r = payRecords.get(i);
                System.out.printf(Locale.CHINA, "%d. 商品:%s 支付方式:%s 点击:%d 购买:%d 支付率:%.4f%n",
                        i + 1, r.itemId, r.payType, r.clickCount, r.payCount, r.payRate);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (collectStmt != null) collectStmt.close(); } catch (Exception ignored) {}
            try { if (payStmt != null) payStmt.close(); } catch (Exception ignored) {}
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (connection != null) connection.close(); } catch (Exception ignored) {}
        }
    }

    // 内部类
    private static class CollectRecord {
        String itemId; int clickCount, collectCount, cartCount; double collectRate;
        CollectRecord(String itemId, int clickCount, int collectCount, int cartCount, double collectRate) {
            this.itemId = itemId; this.clickCount = clickCount; this.collectCount = collectCount;
            this.cartCount = cartCount; this.collectRate = collectRate;
        }
    }

    private static class PayRecord {
        String itemId, payType; int clickCount, payCount; double payRate;
        PayRecord(String itemId, String payType, int clickCount, int payCount, double payRate) {
            this.itemId = itemId; this.payType = payType; this.clickCount = clickCount;
            this.payCount = payCount; this.payRate = payRate;
        }
    }
}
