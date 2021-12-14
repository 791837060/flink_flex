package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import com.atguigu.hotitems_analysis.dto.TradeStatRsp;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class AccountTrade1hPro {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql_BigSorter.txt");
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql_SORT");



        AtomicReference<Long> total = new AtomicReference<>(0L);
        DataStream<String> dataStream = inputStream
                                                .map(line -> {
                                                    Long aLong = total.get();
                                                    aLong = aLong+1;

                                                    if(aLong%100000==0){
                                                        System.out.println(aLong);
                                                    }

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
         /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TradeFlatDto>(Time.seconds(5)) {//有界无序时间戳提取器 乱序数据
             @Override
             public long extractTimestamp(TradeFlatDto element) {
                 return element.getTransTime();
             }
         });*/
         /*.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeFlatDto>forBoundedOutOfOrderness(Duration.ofSeconds(50))
                                                .withTimestampAssigner((SerializableTimestampAssigner<TradeFlatDto>) (element, recordTimestamp) -> {
                                                    return element.getTransTime(); //EventTime Field
                                                }));*/
         .assignTimestampsAndWatermarks(new WatermarkStrategy<TradeFlatDto>() {
             @Override
             public WatermarkGenerator<TradeFlatDto> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                 return new WatermarkGenerator<TradeFlatDto>() {
                     private final long maxOutOfOrderness = 3500;

                     private long currentMaxTimestamp;

                     @Override
                     public void onEvent(TradeFlatDto event, long eventTimestamp, WatermarkOutput output) {
                         currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                         output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
                     }

                     @Override
                     public void onPeriodicEmit(WatermarkOutput output) {

                         output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
                     }
                 };
             }
         }.withTimestampAssigner(new SerializableTimestampAssigner<TradeFlatDto>() {
             @Override
             public long extractTimestamp(TradeFlatDto element, long recordTimestamp) {
                 return element.getTransTime();
             }
         }));
         /*.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TradeFlatDto>() {
             @Override
             public long extractAscendingTimestamp(TradeFlatDto element) {
                 return element.getTransTime();
             }
         });*/

        // trade stat
        TradeStatAggregateFunction tradeStatAggregateFunction = new TradeStatAggregateFunction();
        TradeStatWindowFunction7 tradeStatWindowFunction7 = new TradeStatWindowFunction7();

        DataStream<TradeStatRsp> tradeStatRspStream =accountDataStream
                .keyBy(new AccountTradeStatKeySelector())
                .timeWindow(Time.hours(1L))
                //.trigger(new Trigger1())
                //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))// 连续处理时间触发
                .aggregate(tradeStatAggregateFunction, tradeStatWindowFunction7);

        //tradeStatRspStream.print();
        tradeStatRspStream.writeAsText("/Users/zenghuikang/Downloads/account_stat_1h_pro.sql");

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



}
