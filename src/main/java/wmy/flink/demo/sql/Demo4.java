package wmy.flink.demo.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * className: Demo4
 * package: wmy.flink.demo.sql
 *
 * @date: 2021/8/11
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: 案例：使用事件事件+Watermaker+window完成订单统计
 */
public class Demo4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<Order> orderDataStreamSource = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> context) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    context.collect(order);
                }
            }
            @Override
            public void cancel() {
                isRunning = false;
            }
        }).name("orderDataStreamSource");

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = orderDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order order, long l) {
                                return order.getCreateTime();
                            }
                        })).name("orderSingleOutputStreamOperator");

        tableEnv.createTemporaryView("t_order", orderSingleOutputStreamOperator, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());
        Table table = tableEnv.sqlQuery("select  userId, count(orderId) as orderCount, max(money) as maxMoney,min(money) as minMoney\n" +
                "from t_order\n" +
                "group by userId,\n" +
                "tumble(createTime, interval 5 second)");

        DataStream<Tuple2<Boolean, Row>> orderDataStream = tableEnv.toRetractStream(table, Row.class);
        orderDataStream.print();

        env.execute("orderDataStream >>> ");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime; // 事件时间
    }
}
