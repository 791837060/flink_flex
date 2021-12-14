package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import com.atguigu.hotitems_analysis.dto.TradeStatRsp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class CoinFlexTradeJob5 {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
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

        String UID_PREFIX = "trade-stat-";
        SingleOutputStreamOperator<OrderTick> orderStream = dataStream.process( new OrderTickProcessFunctionCsv() ).uid(UID_PREFIX + "process");

        DataStream<TradeFlatDto> accountDataStream = orderStream.uid(UID_PREFIX + "stream")
         .map(new TradeStatFlatMapFunction()).uid(UID_PREFIX + "flat")
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

         .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TradeFlatDto>() {
             @Override
             public long extractAscendingTimestamp(TradeFlatDto element) {
                 return element.getTransTime();
             }
         });

        // trade stat
        TradeStatAggregateFunction tradeStatAggregateFunction = new TradeStatAggregateFunction();
        TradeStatWindowFunction tradeStatWindowFunction = new TradeStatWindowFunction();

        DataStream<TradeStatRsp> tradeStatRspStream =accountDataStream
                .keyBy(new TradeStatKeySelector())
                .timeWindow(Time.hours(1L))
                 //.timeWindow(Time.hours(1), Time.seconds(5))
                 //.trigger(new Trigger1())
                //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))// 连续处理时间触发
                //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))// 连续事件时间触发
                //.trigger(ContinuousEventTimeTrigger.of(Time.hours(1)))// 连续事件时间触发
                .aggregate(tradeStatAggregateFunction, tradeStatWindowFunction);

        tradeStatRspStream.print();

        // 将各分区数据汇总起来
        /*DataStream<TradeStatRsp> resultStream = tradeStatRspStream
               .keyBy(TradeStatRsp::getWindowEndTime)
               .process(new TotalSum());*/

        //tradeStatRspStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_002.sql");
        /*resultStream.print();*/

        // fee stat
        /*String FEE_UID_PREFIX = "fee-stat-";
        accountDataStream.keyBy(new FeeStatKeySelector())
                .timeWindow(Time.hours(1L))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
                .aggregate(new FeeStatAggregateFunction(), new FeeStatWindowFunction()).uid(FEE_UID_PREFIX + "aggregate")
                .addSink(new FlinkPulsarProducer<>(serviceUrl,
                        feeOutTopic,
                        new AuthenticationDisabled(),
                        FeeStatResp::toJSONBytes,
                        null, null)
                ).uid(FEE_UID_PREFIX + "sink")
                .name(feeOutTopic).uid(FEE_UID_PREFIX + "out");*/

        env.execute("Pulsar Coinflex Trade Job.");
    }

    // 自定义触发器
    public static class Trigger1 extends Trigger<TradeFlatDto, TimeWindow> {
        @Override
        public TriggerResult onElement(TradeFlatDto element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // 每一条数据来到，直接触发窗口计算
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }


    // 实现自定义处理函数，把相同窗口分组统计的count值叠加
    public static class TotalSum extends KeyedProcessFunction<Long, TradeStatRsp, TradeStatRsp>{
        // 定义状态，保存当前
        TradeStatRsp tradeStatRsp;
        @Override
        public void processElement(TradeStatRsp value, Context ctx, Collector<TradeStatRsp> out) throws Exception {
            tradeStatRsp = value;
            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TradeStatRsp> out) throws Exception {
            out.collect(tradeStatRsp);
        }
    }

}
