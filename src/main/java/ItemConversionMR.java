import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ItemConversionMR {

    // 统计每个商品的点击、收藏、加购、支付宝购买次数
    public static class ItemConversionMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text itemKey = new Text();
        private final Text actionValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 把一行转换成字符串
            String line = value.toString();

            // 按特殊分隔符 \u0001 切分
            String[] fields = line.split("\u0001");

            // 只处理至少包含 3 列（item_id、user_id、action）的记录
            if (fields.length > 2) {

                String itemId = fields[0].trim();
                String action = fields[2].trim();

                if (itemId.isEmpty() || action.isEmpty()) {
                    return;
                }

                // 行为统一转成小写，方便比较
                String normalizedAction = action.toLowerCase();

                // key 为 itemID
                itemKey.set(itemId);

                switch (normalizedAction) {
                    case "click":
                        actionValue.set("CLICK");
                        context.write(itemKey, actionValue);
                        break;
                    case "collect":
                        actionValue.set("COLLECT");
                        context.write(itemKey, actionValue);
                        break;
                    case "cart":
                        actionValue.set("CART");
                        context.write(itemKey, actionValue);
                        break;
                    case "alipay":
                        actionValue.set("ALIPAY");
                        context.write(itemKey, actionValue);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    // Reducer 输出每商品的点击、收藏、加购和支付宝购买次数
    public static class ItemConversionReducer extends Reducer<Text, Text, Text, Text> {

        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int clickCount = 0;
            int collectCount = 0;
            int cartCount = 0;
            int alipayCount = 0;

            for (Text val : values) {
                String action = val.toString();
                if ("CLICK".equals(action)) {
                    clickCount++;
                } else if ("COLLECT".equals(action)) {
                    collectCount++;
                } else if ("CART".equals(action)) {
                    cartCount++;
                } else if ("ALIPAY".equals(action)) {
                    alipayCount++;
                }
            }

            // 合并 value：点击数\t收藏数\t加购数\t支付宝支付次数
            String resultValue = clickCount + "\t" + collectCount + "\t" + cartCount + "\t" + alipayCount;

            result.set(resultValue);
            context.write(key, result);
        }
    }
}