package com.shuofxz;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class SQLWordCount_114 {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                // .inStreamingMode()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 读取一行模拟数据作为输入
        String words = "hello world flink flink flink";
        String[] split = words.split("\\W+");

        ArrayList<WC> list = new ArrayList<>();

        for (String word : split) {
            WC wc = new WC(word, 1);
            list.add(wc);
        }

        // create a DataSet from the input data
        Table inputTable = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("word", DataTypes.STRING()),
                        DataTypes.FIELD("frequency", DataTypes.INT())),
                list.stream()
                        .map(wc -> Row.of(wc.word, wc.frequency))
                        .collect(Collectors.toList()));

        // register the input data as a table
        tableEnv.createTemporaryView("WordCount", inputTable);

        // execute a SQL query
        String sql = "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word";
        Table resultTable = tableEnv.sqlQuery(sql);
        resultTable.execute().print();

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
