package com.atguigu.hotitems_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.market_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/17 9:34
 */

import com.atguigu.hotitems_analysis.OrderTick;
import com.atguigu.hotitems_analysis.OrderTickProcessFunctionCsv;
import com.atguigu.hotitems_analysis.TradeStatFlatMapFunction;
import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import com.atguigu.hotitems_analysis.dto.TradeStatRsp;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @ClassName: AppMarketingByChannel
 * @Description:
 * @Author: wushengran on 2020/11/17 9:34
 * @Version: 1.0
 */
public class Statistics1h {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql_BigSorter.txt");
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql_SORT");

        AtomicReference<Integer> total = new AtomicReference<>(0);
        AtomicReference<Boolean> start = new AtomicReference<>(false);
        DataStream<String> dataStream = inputStream
                                                .map(line -> {

                                                    if(line.indexOf("ordertimestamp")==-1){
                                                        return line;
                                                    }else{
                                                        //System.out.println("未知情况"+line);
                                                        if(!"".equals(line)){
                                                            File file =new File("javaio-appendfile.txt");
                                                            //if file doesnt exists, then create it
                                                            if(!file.exists()){
                                                                file.createNewFile();
                                                            }
                                                            //true = append file
                                                            FileWriter fileWritter = new FileWriter(file.getName(),true);
                                                            fileWritter.write(line+"\r\n");
                                                            fileWritter.close();
                                                        }
                                                        return "";
                                                    }
                                                }).filter(data -> !"".equals(data));

        //dataStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_001.sql");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        String UID_PREFIX = "trade-stat-";
        SingleOutputStreamOperator<OrderTick> orderStream = dataStream.process( new OrderTickProcessFunctionCsv() ).uid(UID_PREFIX + "process");

        DataStream<TradeFlatDto> accountDataStream = orderStream.uid(UID_PREFIX + "stream")
                                                             .map(new TradeStatFlatMapFunction()).uid(UID_PREFIX + "flat")
        .assignTimestampsAndWatermarks(new WatermarkStrategy<TradeFlatDto>() {
            @Override
            public WatermarkGenerator<TradeFlatDto> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<TradeFlatDto>() {
                    private long maxTimeStamp = 0;

                    @Override
                    public void onEvent(TradeFlatDto event, long eventTimestamp, WatermarkOutput output) {
                        maxTimeStamp = Math.max(maxTimeStamp, event.getTransTime());
                        //System.out.println("maxTimeStamp:" + maxTimeStamp + "...format:" + sdf.format(maxTimeStamp));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        //                        System.out.println(".....onPeriodicEmit....");
                        long maxOutOfOrderness = 1000;
                        Watermark watermark = new Watermark(maxTimeStamp - maxOutOfOrderness);
                        //                        System.out.println("水印时间："+watermark.getTimestamp()+",eventtime="+eventtime);
                        output.emitWatermark(watermark);
                    }
                };
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<TradeFlatDto>() {
            @Override
            public long extractTimestamp(TradeFlatDto element, long recordTimestamp) {
                return element.getTransTime();
            }
        }));

                                                             /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TradeFlatDto>(Time.seconds(5)) {//有界无序时间戳提取器 乱序数据
                                                                 @Override
                                                                 public long extractTimestamp(TradeFlatDto element) {
                                                                     return element.getTransTime();
                                                                 }
                                                             });*/
                                                             /*.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeFlatDto>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                                                                    .withTimestampAssigner((SerializableTimestampAssigner<TradeFlatDto>) (element, recordTimestamp) -> {
                                                                                                        return element.getTransTime(); //EventTime Field
                                                                                                    }));*/

                                                             /*.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TradeFlatDto>() {
                                                                 @Override
                                                                 public long extractAscendingTimestamp(TradeFlatDto element) {
                                                                     return element.getTransTime();
                                                                 }
                                                             });*/

        // 2. 分渠道开窗统计
        SingleOutputStreamOperator<BigDecimal> resultStream = accountDataStream.keyBy(new TradeStatKeySelector())
                                                                                 //.timeWindow(Time.hours(1), Time.seconds(5))    // 定义滑窗
                                                                                 .timeWindow(Time.hours(1))
                                                                                 .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();

        env.execute("app marketing by channel job");
    }


    // 实现自定义的增量聚合函数
    public static class MarketingCountAgg implements AggregateFunction<TradeFlatDto, BigDecimal, BigDecimal>{
        @Override
        public BigDecimal createAccumulator() {
            return new BigDecimal("0");
        }

        @Override
        public BigDecimal add(TradeFlatDto value, BigDecimal accumulator) {
            return accumulator.add(value.getMatchedQty());
        }

        @Override
        public BigDecimal getResult(BigDecimal accumulator) {
            return accumulator;
        }

        @Override
        public BigDecimal merge(BigDecimal a, BigDecimal b) {
            return null;
        }
    }

    // 实现自定义的全窗口函数
    //<IN, OUT, KEY, W extends Window>
    public static class MarketingCountResult extends ProcessWindowFunction<BigDecimal, BigDecimal, Tuple5<String, String, String, String, String>, TimeWindow>{
        @Override
        public void process(Tuple5<String, String, String, String, String> tuple, Context context, Iterable<BigDecimal> elements, Collector<BigDecimal> out) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            BigDecimal count = elements.iterator().next();

            out.collect(count);
        }
    }
}
