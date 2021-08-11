package wmy.flink.demo.timer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * className: Splitter
 * package: wmy.flink.demo.timer
 *
 * @date: 2021/8/10
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption:  创建FlatMapFunction的实现类Splitter，作用是将字符串分割后生成多个Tuple2实例，f0是分隔后的单词，f1等于1：
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

        if(StringUtils.isNullOrWhitespaceOnly(s)) {
            System.out.println("invalid line");
            return;
        }

        for(String word : s.split(" ")) {
            collector.collect(new Tuple2<String, Integer>(word, 1));
        }
    }
}
