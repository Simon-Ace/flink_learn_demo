package com.shuofxz.c22_flink_practical_project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // 设置消费组
        properties.setProperty("group.id", "group_test");
        // 消费单个 topic，如果要消费多个 topic 第一个参数改为 List<String> 即可
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink-test", new SimpleStringSchema(), properties);
        // 设置从最早的 offset 消费
        consumer.setStartFromEarliest();

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                System.out.println(value);
            }
        });
        env.execute("start consumer...");
    }
}
