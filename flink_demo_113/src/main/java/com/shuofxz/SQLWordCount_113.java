package com.shuofxz;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.ArrayList;

public class SQLWordCount_113 {
    public static void main(String[] args) throws Exception {
        // 创建上下文环境 Flink 1.13 之前
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        // 读取一行模拟数据作为输入
        String words = "hello world flink flink flink";
        String[] split = words.split("\\W+");

        ArrayList<WC> list = new ArrayList<>();

        for (String word : split) {
            WC wc = new WC(word, 1);
            list.add(wc);
        }

        DataSource<WC> input = fbEnv.fromCollection(list);

        // DataSet 转 SQL，指定字段名
        Table table = fbTableEnv.fromDataSet(input, "word,frequency");
        table.printSchema();

        // 注册为一个表
        fbTableEnv.createTemporaryView("WordCount", table);

        Table table1 = fbTableEnv.sqlQuery("select word as word, sum(frequency) as frequency from WordCount group by word");

        DataSet<WC> ds1 = fbTableEnv.toDataSet(table1, WC.class);
        ds1.printToErr();
    }

    public static class WC {
        public String word;
        public long frequency;

        public WC() {}

        public WC(String word, long frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return  word + ", " + frequency;
        }
    }
}
