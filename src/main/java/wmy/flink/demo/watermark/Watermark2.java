package wmy.flink.demo.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.operations.command.ResetOperation;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * className: Watermark2
 * package: wmy.flink.demo.watermark
 *
 * @date: 2021/8/11
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: 自定义一个watermark
 */
public class Watermark2 {
    public static void main(String[] args) {
        FastDateFormat df = FastDateFormat.getInstance("HH:mm:ss");

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
                    int money = random.nextInt(1000);
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    Thread.sleep(1000);
                    Order order = new Order();
                    order.setOrderId(orderId);
                    order.setUserId(userId);
                    order.setMoney(money);
                    order.setEventTime(eventTime);
                    context.collect(order);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(((order, l) -> order.eventTime))
        );

        // 自定义水位线
        SingleOutputStreamOperator<Order> orderWatermarkDS = orderDS
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Order>() {
                            @Override
                            public WatermarkGenerator<Order> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Order>() {
                                    private int userId = 0;
                                    private long eventTime = 0L;
                                    private final long outOfOrdernessMills = 3000;
                                    private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMills + 1;

                                    @Override
                                    public void onEvent(Order order, long eventTimestamp, WatermarkOutput watermarkOutput) {
                                        userId = order.userId;
                                        eventTime = order.eventTime;
                                        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                        // Watermark = 当前最大事件事件 - 最大运行的延迟时间或乱序时间
                                        Watermark watermark = new Watermark(maxTimestamp - outOfOrdernessMills - 1);
                                        System.out.println(
                                                "key: " + userId +
                                                        " , 系统时间：" +
                                                        df.format(System.currentTimeMillis()) +
                                                        " , 事件时间：" + df.format(eventTime) +
                                                        " , 水印时间：" + df.format(eventTime) + ",水印时间:" + df.format(watermark.getTimestamp()));

                                        watermarkOutput.emitWatermark(watermark);
                                    }
                                };
                            }
                        }.withTimestampAssigner(((order, l) -> order.eventTime))
                );
    }

    // TODO 定义一个对象
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
