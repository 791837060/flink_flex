package com.atguigu.hotitems_analysis.lemon;

import com.atguigu.hotitems_analysis.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class Csv2InsertLemon {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/csvtest.csv");
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_001.sql_SORT");



        AtomicReference<Long> total = new AtomicReference<>(0L);
        DataStream<String> dataStream = inputStream
                                                .map(line -> {
                                                    /*
                                                    orderid,marketid,accountid,marketcode,orderside,ordertype,ordertimestamp,lastupdated,lasttradetimestamp,timeinforce,
                                                    clientorderid,status,lastmatchedorderid,lastmatchedorderid2,leg1_price,leg2_price,matchedid,matchedtype,quantity,remainingqty,
                                                    price,triggerprice,triggerlimit,fees,feeinstrumentid,tradetype,base,counter,market_type,is_triggered,
                                                    is_liquidation,source,price_test,quantity_test,tradetype_test,matchedtype_test

                                                    304410084292095784,2001011000000,95253,BTC-USD-SWAP-LIN,BUY,MARKET,1618069198550,2021-04-10 15:39:58.639,2021-04-10 15:39:58.639,IOC,
                                                    1136756662021,PARTIAL_FILL,304410084292095772,0,NULL,NULL,304410084292095785,TAKER,0.400000000,0.100000000,
                                                    60283.500000000,NULL,NULL,7.234020000,USD,FUTURE,BTC,USD,NULL,false,
                                                    NULL,NULL,NULL,NULL,TRADE,NULL*/

                                                    Long aLong = total.get();
                                                    aLong = aLong+1;
                                                    total.set(aLong);
                                                    if(aLong%1000000==0){
                                                        System.out.println("已完成==>"+aLong/100000000.0);
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

        SingleOutputStreamOperator<AccountMon> orderStream = dataStream.process( new CsvAccountIdSql());

        DataStream<String> accountWindowStream =orderStream
                                                        .keyBy(AccountMon::getMod)
                                                        .process(new Csv2InsertLemon.AccountFunction());

        //accountWindowStream.print();
        accountWindowStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_lemon_100.sql");

        env.execute("Pulsar Coinflex Trade Job.");
    }

    // 实现自定义的全窗口函数
    public static class AccountFunction extends KeyedProcessFunction<Long, AccountMon, String> {
        Map<String,Integer> mapInt = new HashMap<>();
        Map<String,String> mapString = new HashMap<>();
        int total =0;
        String insertStr ="INSERT INTO cf_trade_history ("+
                                  "orderid,marketid,accountid,marketcode,orderside,ordertype,ordertimestamp,lastupdated,lasttradetimestamp,timeinforce,"
                                  + "clientorderid,status,lastmatchedorderid,lastmatchedorderid2,leg1_price,leg2_price,matchedid,matchedtype,quantity,remainingqty,"
                                  + "price,triggerprice,triggerlimit,fees,feeinstrumentid,tradetype,base,counter,market_type,is_triggered,"
                                  + "is_liquidation,source) VALUES ";
        @Override
        public void processElement(AccountMon in, Context context, Collector<String> out) throws Exception {
            if(mapString.get(in.getMod()+"")==null){
                mapString.put(in.getMod()+"",insertStr);
            }
            if(mapInt.get(in.getMod()+"")==null){
                mapInt.put(in.getMod()+"",0);
            }

            total =total +1;

            //(16030917120320808, 17001011000000, 809973, 'UNI-USD-SWAP-LIN', 'SELL', 'LIMIT', 1604708298287, '2020-11-07 00:18:18.625+00:00', '2020-11-07 00:18:18.625+00:00', 'GTC',
            // 1604708298135, 'FILLED', 16030917120320799, 0, 16030917120320809, 'TAKER', 6.000000000, 0E-9, 2.680000000, NULL,
            // NULL, 0.068900000, 'FLEX', NULL, NULL, 'TRADE', 'UNI', 'USD', NULL, false,
            // NULL, NULL)

            mapString.put(in.getMod()+"",mapString.get(in.getMod()+"")+ "(" +in.getValue()+ "),\n\r");
            mapInt.put(in.getMod()+"",mapInt.get(in.getMod()+"")+1);

            //1000026499008,2001011000000,95253,BTC-USD-SWAP-LIN,SELL,MARKET,1626537418070,2021-07-17 15:56:58.101,2021-07-17 15:56:58.084,IOC,
            // 1136756662019,FILLED,1000026506964,0,NULL,NULL,304493660120424777,TAKER,0.130000000,0E-9,
            // 31738.500000000,NULL,NULL,3.300804000,USD,FUTURE,BTC,USD,NULL,false,
            // false,NULL,NULL,NULL,TRADE,NULL

            if(total == 1000 ||(in.getValue().indexOf("1000026499008")!=-1&&in.getValue().indexOf("304493660120424777")!=-1)){
                Collection<String> keys = mapString.keySet();
                for (String key :keys) {
                    mapString.put(key,mapString.get(key).substring(0,mapString.get(key).lastIndexOf(")")+1)+";");
                    mapString.put(key,mapString.get(key).replace("cf_trade_history","cf_trade_history_"+key));
                    String s = mapString.get(key + "");

                    total = 0;
                    mapInt.put(key,0);
                    mapString.put(key,insertStr);
                    if(s.indexOf(") VALUES")!=-1){
                        out.collect(s);
                    }
                }
            }
        }
    }


}
