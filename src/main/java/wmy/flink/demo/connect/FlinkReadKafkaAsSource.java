package wmy.flink.demo.connect;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;


/**
 * className: FlinkReadKafkaAsSource
 * package: wmy.flink.demo.connect
 *
 * @date: 2021/8/10
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption:
 */
public class FlinkReadKafkaAsSource {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);


        // 读取kafkasource
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("yaxin01:9092")
                .setTopics("connectDemo")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("yaxin01:9092")
                .setTopics("connectDemo")
                .setGroupId("jlfdjalsdjflads")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<String> kafkaSource = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "source1").name("source1");
        SingleOutputStreamOperator<String> kafkaSource1 =  env.fromSource(source2, WatermarkStrategy.noWatermarks(), "source2").name("source2");

        KeyedStream<OrderBean, String> keyedStream1 = kafkaSource.map(new RichMapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                String[] fields = value.split(",");
                OrderBean orderBean = new OrderBean();
                orderBean.setOrderId(fields[0].trim());
                orderBean.setUserId(fields[1].trim());
                orderBean.setGoodsId(fields[2].trim());
                orderBean.setPrice(Double.parseDouble(fields[3].trim()));
                orderBean.setTime(Long.parseLong(fields[4].trim()));
                return orderBean;
            }
        }).keyBy(OrderBean::getOrderId);

        KeyedStream<OrderBean, String> keyedStream2 = kafkaSource1.map(new RichMapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                String[] fields = value.split(",");
                OrderBean orderBean = new OrderBean();
                orderBean.setOrderId(fields[0].trim());
                orderBean.setUserId(fields[1].trim());
                orderBean.setGoodsId(fields[2].trim());
                orderBean.setPrice(Double.parseDouble(fields[3].trim()));
                orderBean.setTime(Long.parseLong(fields[4].trim()));
                return orderBean;
            }
        }).keyBy(OrderBean::getOrderId);

        // 定义侧输出流标签
        OutputTag<OrderBean> keyedStream11 = new OutputTag<OrderBean>("keyedStream1"){};
        OutputTag<OrderBean> keyedStream12 = new OutputTag<OrderBean>("keyedStream2"){};

        SingleOutputStreamOperator<Tuple2<OrderBean, OrderBean>> streamOperator = keyedStream1
                .connect(keyedStream2)
                .process(new CoProcessFunction<OrderBean, OrderBean, Tuple2<OrderBean, OrderBean>>() {
                    // 定义流1的状态
                    ValueState<OrderBean> state1;
                    // 定义流2的状态
                    ValueState<OrderBean> state2;

                    // 定义一个用于删除定时器的状态
                    //ValueState<Long> timeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state1 = getRuntimeContext().getState(new ValueStateDescriptor<OrderBean>("state1", OrderBean.class));
                        state2 = getRuntimeContext().getState(new ValueStateDescriptor<OrderBean>("state1", OrderBean.class));
                        //timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
                    }

                    @Override
                    public void processElement1(OrderBean value1, CoProcessFunction<OrderBean, OrderBean, Tuple2<OrderBean, OrderBean>>.Context context, Collector<Tuple2<OrderBean, OrderBean>> collector) throws Exception {
                        OrderBean value2 = state2.value();
                        if (value2 != null) {
                            collector.collect(Tuple2.of(value1, value2));
                            // 清空流2对用的state信息
                            state2.clear();
                            // 流2来了就可以删除定时器，并把定时器的状态清除
                            //context.timerService().deleteEventTimeTimer(timeState.value());
                            //timeState.clear();
                        } else {
                            // 流2的状态还没有来
                            state1.update(value1);
                            // 并注册一个1分钟的定时器，流中的eventTime * 60s
                            long time = value1.getTime() + 60 * 1000;
                            //timeState.update(time);
                            //context.timerService().registerEventTimeTimer(time);
                        }
                    }

                    @Override
                    public void processElement2(OrderBean value1, CoProcessFunction<OrderBean, OrderBean, Tuple2<OrderBean, OrderBean>>.Context context, Collector<Tuple2<OrderBean, OrderBean>> collector) throws Exception {
                        OrderBean value2 = state1.value();
                        if (value1 != null) {
                            collector.collect(Tuple2.of(value2, value1));
                            state1.clear();
                            //context.timerService().deleteEventTimeTimer(timeState.value());
                            //timeState.clear();
                        } else {
                            state2.update(value1);
                            long time = value1.getTime() + 60 * 1000;
                            //timeState.update(time);
                            //context.timerService().registerEventTimeTimer(time);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<OrderBean, OrderBean, Tuple2<OrderBean, OrderBean>>.OnTimerContext ctx, Collector<Tuple2<OrderBean, OrderBean>> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        // 定时器出发了，即一分钟内没有收到两个流
                        // 流1不为空，则将流1侧输出
                        //if (state1.value() != null) {
                        //    ctx.output(keyedStream11, state1.value());
                        //}
                        //if (state2.value() != null) {
                        //    ctx.output(keyedStream12, state2.value());
                        //}
                        state1.clear();
                        state2.clear();
                    }
                }).name("connect source1 and source2");

        streamOperator.print("主流数据 ---> ");
        //streamOperator.getSideOutput(keyedStream11).print("第一个流的数据 ---> ");
        //streamOperator.getSideOutput(keyedStream12).print("第二个流的数据 ---> ");


        env.execute("FlinkReadKafkaAsSource ---> wmy-version-1.0");
    }
}
