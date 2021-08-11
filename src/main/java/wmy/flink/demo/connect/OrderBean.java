package wmy.flink.demo.connect;

import lombok.*;

/**
 * className: OrderBean
 * package: wmy.flink.demo.connect
 *
 * @date: 2021/8/10
 * @author: 数仓开发工程师
 * @email: wmy_2000@163.com
 * @Company: 亚信技术有限公司
 * @blog: https://wmybigdata-1.github.io/
 * @Descirption:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
@Setter
public class OrderBean {
    private String orderId;
    private String userId;
    private String goodsId;
    private Double price;
    private Long time;
}
