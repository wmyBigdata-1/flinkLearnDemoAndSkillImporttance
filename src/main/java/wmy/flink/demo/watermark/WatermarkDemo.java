package wmy.flink.demo.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * className: WatermarkDemo
 * package: wmy.flink.demo.watermark
 *
 * @date: 2021/8/11
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: watermark案例演示
 */
public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStreamSource<Order> orderDS = env.addSource(new SourceFunction<Order>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> context) throws Exception {
                Random random = new Random();
                while (flag) {
                    String orderId = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(101);
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    context.collect(new Order(orderId, userId, money, eventTime));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        // TODO 求每个用户订单的总金额
        // 设置水位线 = 最大的事件事件 - 延迟时间
        SingleOutputStreamOperator<Order> orderDSWithWatermark = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Order>() {
                                    @Override
                                    public long extractTimestamp(Order order, long l) {
                                        return order.getEventTime();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<Order> result = orderDSWithWatermark
                .keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money");

        result.print();

        env.execute("result");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }

}
