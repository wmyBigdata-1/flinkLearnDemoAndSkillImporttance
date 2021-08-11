package wmy.flink.demo.timer;

import lombok.Data;

/**
 * className: CountWithTimestamp
 * package: wmy.flink.demo.timer
 *
 * @date: 2021/8/10
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption: 创建bean类CountWithTimestamp，里面有三个字段
 */
@Data
public class CountWithTimestamp {
    public String key;
    public long count;
    public long lastModified;
}
