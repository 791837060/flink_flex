package com.atguigu.hotitems_analysis.pro;


import com.github.davidmoten.bigsorter.Serializer;
import com.github.davidmoten.bigsorter.Sorter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class BigSorter {


  public static void main(String args[]) {
    /*try {

coinflex_cf_trade_history_2021-12-06_Sorter.sql
    } catch(IOException ex) {
      ex.printStackTrace();
    }*/
    //String fileName = "/Users/zenghuikang/workspace/coinflex/atguigu/flink/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/ESort.txt_BigSorter";
    //String fileNameOut = "/Users/zenghuikang/workspace/coinflex/atguigu/flink/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/BigSorter.txt";

    String fileName = "/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.csv";
    String fileNameOut = "/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06_sorter.csv";
//INSERT INTO cf_trade_history_91 (orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2, matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source) VALUES 	(

    //long num = Long.valueOf(split[2].trim())%100;
    //newLine =newLine.replace("cf_trade_history","cf_trade_history_"+num);
    Serializer<CSVRecord> serializer =
            Serializer.csv(
                    CSVFormat
                            .DEFAULT
                            .withFirstRecordAsHeader()
                            .withRecordSeparator("\n"),
                    StandardCharsets.UTF_8);
    Comparator<CSVRecord> comparator = (x, y) -> {
      long a = Long.parseLong(x.get("ordertimestamp"));
      long b = Long.parseLong(y.get("ordertimestamp"));
      return Long.compare(a, b);
    };
    Sorter
            .serializer(serializer)
            .comparator(comparator)
            .input(new File(fileName))
            .output(new File(fileNameOut))
            .maxFilesPerMerge(10000) // default is 100
            .maxItemsPerFile(1000000) // default is 100,000
            .sort();


  }
}