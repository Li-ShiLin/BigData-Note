package com.action.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {

    public static void main(String[] args) {

        //1.获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用FlinkCDC SQL方式建表
        tableEnv.executeSql("" +
                "create table t1(\n" +
                "    id string primary key NOT ENFORCED,\n" +
                "    name string" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'server02',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'PS666666',\n" +
                " 'database-name' = 'test',\n" +
                " 'table-name' = 't1'\n" +
                ")");

        //3.查询并打印
        Table table = tableEnv.sqlQuery("select * from t1");
        table.execute().print();

    }
}
