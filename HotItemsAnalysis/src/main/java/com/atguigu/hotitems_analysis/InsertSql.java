package com.atguigu.hotitems_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/14 15:16
 */

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @ClassName: HotItems
 * @Description:
 * @Author: wushengran on 2020/11/14 15:16
 * @Version: 1.0
 */
public class InsertSql {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/Downloads/coinflex_tx_account_transfer_2021-10-25_08_53_13.sql");

        AtomicReference<String> lineBuf = new AtomicReference<>("");
        AtomicReference<Integer> count = new AtomicReference<>(0);
        AtomicReference<Integer> all = new AtomicReference<>(0);
        AtomicReference<Integer> del = new AtomicReference<>(0);
        AtomicReference<Integer> total = new AtomicReference<>(0);
        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<String> dataStream = inputStream
                .map(line -> {
                    //最后是45条
                    if(line.indexOf("INSERT INTO tx_account_transfer")!=-1){
                        lineBuf.set("");
                        count.set(0);

                        all.set(all.get()+100);
                        //System.out.println(all.get());
                        String table =line.replace("tx_account_transfer","tx_account_transfer_non_fee");
                        lineBuf.set(table);
                    }else if(line.indexOf("9999999999, 'TRADEFEE',")!=-1){
                        count.set(count.get()+1);
                        del.set(del.get()+1);
                        total.set(total.get()+1);
                    }else if(line.endsWith(";")||line.endsWith(",")){
                        lineBuf.set(lineBuf.get()+line);
                        total.set(total.get()+1);
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
                    }


                    if(count.get()==100){
                        return "";
                    }

                    if(line.endsWith(";")){
                        System.out.println("del:"+del.get()+",total:"+total.get());
                        if(lineBuf.get().endsWith(";")){
                            return lineBuf.get();
                        }else{
                            String end = lineBuf.get().substring(0,lineBuf.get().length()-1)+";";
                            return end;
                        }
                    }else{
                        return "";
                    }

                }).filter(data -> !"".equals(data));

        //dataStream.print();
        dataStream.writeAsText("/Users/zenghuikang/Downloads/coinflex_tx_account_transfer_2021-10-25_08_53_13_tx_account_transfer_non_fee1.sql");
        env.execute("hot items analysis");
    }


}
