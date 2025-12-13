package com.action.tableapi;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 常规联结查询示例
 * 演示如何使用常规联结（Regular Join）进行表连接
 *
 * 功能说明：
 * 1. 创建两条数据流，分别代表订单表和商品表
 * 2. 使用 INNER JOIN 进行内联结
 * 3. 使用 LEFT JOIN 进行左外联结
 * 4. 常规联结是更新查询，结果表会有更新操作
 */
public class RegularJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ==================== 1. 创建表环境 ====================
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 2. 创建第一条流：订单表 ====================
        // 订单表包含：订单ID、商品ID、订单时间
        DataStream<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("order-1", "product-1", 1000L),
                Tuple3.of("order-2", "product-2", 2000L),
                Tuple3.of("order-3", "product-1", 3000L)
        );

        // ==================== 3. 创建第二条流：商品表 ====================
        // 商品表包含：商品ID、商品名称、商品价格
        DataStream<Tuple3<String, String, Double>> productStream = env.fromElements(
                Tuple3.of("product-1", "商品A", 99.9),
                Tuple3.of("product-2", "商品B", 199.9),
                Tuple3.of("product-3", "商品C", 299.9)
        );

        // ==================== 4. 将数据流转换成表并注册为虚拟表 ====================
        tableEnv.createTemporaryView("OrderTable", orderStream,
                $("orderId"), $("productId"), $("orderTime"));
        tableEnv.createTemporaryView("ProductTable", productStream,
                $("productId"), $("productName"), $("price"));

        // ==================== 5. 执行内联结查询 ====================
        // INNER JOIN：返回两表中符合联结条件的所有行的组合
        // 联结条件：OrderTable.productId = ProductTable.productId
        Table innerJoinResult = tableEnv.sqlQuery(
                "SELECT o.orderId, o.productId, p.productName, p.price " +
                        "FROM OrderTable o " +
                        "INNER JOIN ProductTable p " +
                        "ON o.productId = p.productId"
        );

        // ==================== 6. 执行左外联结查询 ====================
        // LEFT JOIN：返回左侧表中所有行，以及右侧表中匹配的行
        // 如果右侧表中没有匹配的行，则右侧表的字段为 NULL
        Table leftJoinResult = tableEnv.sqlQuery(
                "SELECT o.orderId, o.productId, p.productName, p.price " +
                        "FROM OrderTable o " +
                        "LEFT JOIN ProductTable p " +
                        "ON o.productId = p.productId"
        );

        // ==================== 7. 将结果表转换成更新日志流并打印输出 ====================
        // 常规联结是更新查询，必须使用 toChangelogStream() 转换
        tableEnv.toChangelogStream(innerJoinResult).print("内联结结果");
        tableEnv.toChangelogStream(leftJoinResult).print("左外联结结果");

        // ==================== 8. 执行程序 ====================
        env.execute();
    }
}

