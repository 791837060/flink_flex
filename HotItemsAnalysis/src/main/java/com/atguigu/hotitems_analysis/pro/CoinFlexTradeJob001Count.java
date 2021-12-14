package com.atguigu.hotitems_analysis.pro;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class CoinFlexTradeJob001Count {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        //grep cf_trade_history_92  /Users/zenghuikang/Downloads/cf_trade_history_001.sql | wc -l
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.sql");
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql");

        AtomicReference<Long> total2 = new AtomicReference<>(0L);
        DataStream<String> dataStream = inputStream
                   .map(line -> {
                       //1632729602201
                       String values = line.substring(line.indexOf("VALUES") + 6, line.lastIndexOf(");"));
                       String substring = values.substring(line.indexOf("(") + 1);
                       String[] split = substring.split(",");
                       for (int j=0;j<split.length;j++) {
                           split[j] = split[j].trim();
                       }
                       String orderTime = split[6];
                       if(line.indexOf("cf_trade_history_92")!=-1&&Long.valueOf(orderTime)<1632729602201L){
                           Long aLong = total2.get();
                           aLong = aLong+1;
                           total2.set(aLong);
                           System.out.println("已完成==>"+aLong+","+aLong);
                           return "";
                       }else{
                           return "";
                       }
                   }).filter(data -> !"".equals(data));
        env.execute("Pulsar Coinflex Trade Job.");
    }

    // 实现自定义的全窗口函数

}
