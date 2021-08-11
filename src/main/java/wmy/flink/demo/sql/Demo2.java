package wmy.flink.demo.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * className: Demo2
 * package: wmy.flink.demo.sql
 *
 * @date: 2021/8/11
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: 使用SQL和Table两种方式做WordCount
 */
public class Demo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<WC> wordsDS = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1)
        );

        tableEnv.createTemporaryView("t_words", wordsDS, $("word"), $("frequency"));
        String sql = "select word,sum(frequency) as frequency\n " +
                "from t_words\n " +
                "group by word";

        Table resultTable = tableEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, WC>> resultDS = tableEnv.toRetractStream(resultTable, WC.class);

        resultDS.print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        public long frequency;
    }
}

