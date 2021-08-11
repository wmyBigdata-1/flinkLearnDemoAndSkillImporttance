package wmy.flink.demo.connect;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import java.io.IOException;
import java.time.Duration;

/**
 * className: Demo
 * package: wmy.flink.demo.connect
 *
 * @date: 2021/8/10
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption:
 */
public class Demo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<OrderBean> source = KafkaSource.<OrderBean>builder()
                .setBootstrapServers("brokers")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AbstractDeserializationSchema<OrderBean>() {
                    @Override
                    public OrderBean deserialize(byte[] bytes) throws IOException {
                        return null;
                    }
                })
                .build();

        DataStreamSource<OrderBean> kafka_source = env.fromSource(source, WatermarkStrategy
                .<OrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderBean>() {
                            @Override
                            public long extractTimestamp(OrderBean orderBean, long l) {
                                return orderBean.getTime();
                            }
                        }
                ), "Kafka Source");

    }
}
