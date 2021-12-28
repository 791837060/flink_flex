package com.atguigu.hotitems_analysis.pro;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicReference;

/**
 * created by zhk
 */
public class CoinFlexTradeJobUpdate3 {

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        //DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.sql");
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/cf_trade_history_update2.sql");

        AtomicReference<Long> total2 = new AtomicReference<>(0L);
        AtomicReference<Boolean> start = new AtomicReference<>(false);
        DataStream<String> dataStream = inputStream
                   .map(line -> {
                       String[] split1 = line.split(";");
                       return split1[0]+";";
                   }).filter(data -> !"".equals(data));

        //dataStream.print();
        dataStream.writeAsText("/Users/zenghuikang/Downloads/cf_trade_history_update.sql");

        //Key (matchedid, orderid)=(304600832838511343, 1000083294535)
        //select * from cf_trade_history_92 where matchedid = 8951502652253133973 and orderid = 1000083294535

        //INSERT INTO cf_trade_history_92 (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2,
        // matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES
        // (1001201468591, 62001031000000, 9999999992, 'LDO-USD-REPO-LIN', 'SELL', 'LIMIT', 1636430403164, '2021-11-09 04:00:03.214+00:00', '2021-11-09 04:00:03.198+00:00', 'AUCTION',
        // 1, 'FILLED', 1001184316819, 0,
        // 8951502652253133973, 'TAKER', 40.000000000, 0E-9, (-0.000002000), NULL, NULL, 0E-9, NULL, 4.189991620, 4.190000000, 'REPO', 'LDO-USD', 'LDO-USD-SWAP-LIN', NULL, false, false, 0);

        env.execute("Pulsar Coinflex Trade Job.");
    }

}
