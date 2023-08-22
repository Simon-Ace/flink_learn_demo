package com.shuofxz.c08_watermark;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GitOriCode {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //设置为eventtime事件类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> dataStream = env
                .socketTextStream("127.0.0.1", 9000)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    private Long currentTimeStamp = 0L;
                    //设置允许乱序时间
                    private Long maxOutOfOrderness = 5000L;

                    @Override
                    public Watermark getCurrentWatermark() {

                        return new Watermark(currentTimeStamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String s, long l) {
                        String[] arr = s.split(",");
                        long timeStamp = Long.parseLong(arr[1]);
                        currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                        System.err.println(s + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp - maxOutOfOrderness));
                        return timeStamp;
                    }
                });

        dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {

                        String[] split = s.split(",");
                        return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .minBy(1)
                .print();

        // dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
        //             @Override
        //             public Tuple2<String, Long> map(String s) throws Exception {
        //
        //                 String[] split = s.split(",");
        //                 return new Tuple2<String, Long>(split[0], Long.parseLong(split[1]));
        //             }
        //         })
        //         .keyBy(0)
        //         .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        //         .apply(new WindowFunction<Tuple2<String, Long>, Object, Tuple, TimeWindow>() {
        //             @Override
        //             public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input,
        //                               Collector<Object> out) throws Exception {
        //                 System.out.println("Window: " + window);
        //                 for (Tuple2<String, Long> item : input) {
        //                     System.out.println(item);
        //                 }
        //             }
        //         });
        //
        env.execute("WaterMark Test Demo");

    }//

}
