package wmy.flink.demo.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * className: Demo01
 * package: wmy.flink.demo.sql
 *
 * @date: 2021/8/11
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: 案例：将DataStream数据转为Table或view使用SQL进行统计查询
 */
public class Demo1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<Order> orderA = env.fromCollection(
                Arrays.asList(
                        new Order(1L, "beer", 3),
                        new Order(1L, "diaper", 4),
                        new Order(3L, "rubber", 2)
                )
        );

        DataStreamSource<Order> orderB = env.fromCollection(
                Arrays.asList(
                        new Order(2L, "pen", 3),
                        new Order(3L, "rubber", 3),
                        new Order(4L, "beer", 1)
                )
        );

        Table tableA = tableEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        tableEnv.createTemporaryView("tableB", orderB, $("user"), $("product"), $("amount"));

        String sql = "select * from " + tableA + " where amount > 2 \n" +
                " union \n" +
                " select * from tableB where amount > 1";
        Table result = tableEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Order>> resultDS = tableEnv.toRetractStream(result, Order.class);
        resultDS.print();

        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private Long user;
        private String product;
        private int amount;
    }
}
