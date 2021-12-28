package com.atguigu.hotitems_analysis.pro;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class Sql2SqlTradeJobLemon {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.sql");

        AtomicReference<Long> total = new AtomicReference<>(0L);
        AtomicReference<Long> total2 = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(false);
        AtomicReference<String> insertStr = new AtomicReference<>("INSERT INTO cf_trade_history (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2, matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES ");
        DataStream<String> dataStream = inputStream
                   .map(line -> {
                       if(line.indexOf("INSERT INTO cf_trade_history")!=-1){
                           start.set(true);
                       }

                       if(!start.get()){
                           return "";
                       }

                       String newLine = "";
                       if(line.indexOf("INSERT INTO cf_trade_history")!=-1){
                           return "";
                       }else if(line.endsWith(",")){
                           Long aLong = total2.get();
                           aLong = aLong+1;
                           total2.set(aLong);
                           if(aLong%1000000==0){
                               System.out.println("已完成==>"+aLong/100000000.0);
                           }
                           newLine += insertStr.get() + line;
                           if(newLine.endsWith(";")){

                           }else{
                               String end = newLine.substring(0,newLine.length()-1)+";";
                               newLine = end;
                           }

                           String[] split = line.split(",");
                           if(!" NULL".equals(split[2])&&!StringUtils.isEmpty(split[2])){
                               long num = Long.valueOf(split[2].trim())%100;
                               newLine =newLine.replace("cf_trade_history","cf_trade_history_"+num);
                               total.set(total.get()+1);
                               return newLine;
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
                               System.out.println("已完成==>"+aLong/100000000.0);
                           }
                           newLine += insertStr.get() + line;
                           String[] split = line.split(",");
                           if(!" NULL".equals(split[2])&&!StringUtils.isEmpty(split[2])){
                               long num = Long.valueOf(split[2].trim())%100;
                               newLine=newLine.replace("cf_trade_history","cf_trade_history_"+num);
                               total.set(total.get()+1);
                               return newLine;
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

        //dataStream.print();
        dataStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_002.sql");

        //Key (matchedid, orderid)=(304600832838511343, 1000083294535)
        //select * from cf_trade_history_92 where matchedid = 8951502652253133973 and orderid = 1000083294535

        //INSERT INTO cf_trade_history_92 (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2,
        // matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES
        // (1001201468591, 62001031000000, 9999999992, 'LDO-USD-REPO-LIN', 'SELL', 'LIMIT', 1636430403164, '2021-11-09 04:00:03.214+00:00', '2021-11-09 04:00:03.198+00:00', 'AUCTION', 1, 'FILLED', 1001184316819, 0,
        // 8951502652253133973, 'TAKER', 40.000000000, 0E-9, (-0.000002000), NULL, NULL, 0E-9, NULL, 4.189991620, 4.190000000, 'REPO', 'LDO-USD', 'LDO-USD-SWAP-LIN', NULL, false, false, 0);

        System.out.println("total.get()==>"+total.get());

        env.execute("Pulsar Coinflex Trade Job.");
    }

}
