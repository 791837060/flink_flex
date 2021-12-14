package com.atguigu.hotitems_analysis.pro;

import com.atguigu.hotitems_analysis.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class CoinFlexTradeJob100 {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        //grep cf_trade_history_92  /Users/zenghuikang/Downloads/cf_trade_history_001.sql | wc -l
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.sql");
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/temp.sql");

        AtomicReference<Long> total = new AtomicReference<>(0L);
        AtomicReference<Long> total2 = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(false);
        DataStream<String> dataStream = inputStream
                   .map(line -> {
                       if(line.indexOf("INSERT INTO cf_trade_history")!=-1){
                           start.set(true);
                       }

                       if(!start.get()){
                           return "";
                       }

                       if(line.indexOf("INSERT INTO cf_trade_history")!=-1){
                           return "";
                       }else if(line.endsWith(",")){
                           Long aLong = total2.get();
                           aLong = aLong+1;
                           total2.set(aLong);
                           if(aLong%1000000==0){
                               System.out.println("已完成==>"+aLong+","+aLong/100000000.0+",total==>"+total);
                           }

                           String[] split = line.split(",");
                           if(!" NULL".equals(split[2])&&!StringUtils.isEmpty(split[2])){
                               total.set(total.get()+1);
                               return line;
                           }else{
                               File file =new File("javaio-appendfile.txt");
                               //if file doesnt exists, then create it
                               if(!file.exists()){
                                   file.createNewFile();
                               }
                               //true = append file
                               FileWriter fileWritter = new FileWriter(file.getName(),true);
                               fileWritter.write(line+"\r\n");
                               fileWritter.close();
                               return "";
                           }

                       }else if(line.endsWith(";")){
                           Long aLong = total2.get();
                           aLong = aLong+1;
                           total2.set(aLong);
                           if(aLong%1000000==0){
                               System.out.println("已完成==>"+aLong+","+aLong/100000000.0+",total==>"+total);
                           }
                           String[] split = line.split(",");
                           if(!" NULL".equals(split[2])&&!StringUtils.isEmpty(split[2])){
                               total.set(total.get()+1);
                               return line;
                           }else{
                               File file =new File("javaio-appendfile.txt");
                               //if file doesnt exists, then create it
                               if(!file.exists()){
                                   file.createNewFile();
                               }
                               //true = append file
                               FileWriter fileWritter = new FileWriter(file.getName(),true);
                               fileWritter.write(line+"\r\n");
                               fileWritter.close();
                               return "";
                           }
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


        SingleOutputStreamOperator<AccountMon> orderStream = dataStream.process( new AccountIdSql());

        DataStream<String> accountWindowStream =orderStream
                     .keyBy(AccountMon::getMod)
                     .process(new AccountFunction());

        accountWindowStream.print();
        //accountWindowStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_100.sql");
        env.execute("Pulsar Coinflex Trade Job.");
    }

    // 实现自定义的全窗口函数
    public static class AccountFunction extends KeyedProcessFunction<Long, AccountMon, String> {
        Map<String,Integer> mapInt = new HashMap<>();
        Map<String,String> mapString = new HashMap<>();
        int total =0;
        @Override
        public void processElement(AccountMon in, Context context, Collector<String> out) throws Exception {
            if(mapString.get(in.getMod()+"")==null){
                mapString.put(in.getMod()+"","INSERT INTO cf_trade_history (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2, matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES ");
            }
            if(mapInt.get(in.getMod()+"")==null){
                mapInt.put(in.getMod()+"",0);
            }

            total =total +1;

            mapString.put(in.getMod()+"",mapString.get(in.getMod()+"")+ "(" +in.getValue()+ "),\n\r");
            mapInt.put(in.getMod()+"",mapInt.get(in.getMod()+"")+1);

            if(total == 1000 ||(in.getValue().indexOf("16030917120320799")!=-1&&in.getValue().indexOf("16030917120320809")!=-1)){
                Collection<String> keys = mapString.keySet();
                for (String key :keys) {
                    mapString.put(key,mapString.get(key).substring(0,mapString.get(key).lastIndexOf(")")+1)+";");
                    mapString.put(key,mapString.get(key).replace("cf_trade_history","cf_trade_history_"+key));
                    String s = mapString.get(key + "");

                    total = 0;
                    mapInt.put(key,0);
                    mapString.put(key,"INSERT INTO cf_trade_history (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2, matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES ");
                    if(s.indexOf(") VALUES")!=-1){
                        out.collect(s);
                    }
                }
            }
        }
    }

}
