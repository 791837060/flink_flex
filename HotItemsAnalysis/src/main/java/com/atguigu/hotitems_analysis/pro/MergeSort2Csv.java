package com.atguigu.hotitems_analysis.pro;

import com.atguigu.hotitems_analysis.dto.OrderByDto;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MergeSort2Csv {
  public static boolean start = false;

  public static long total = 0L;

  private MergeSort2Csv() {
  }
  /**
   * 读指定行数的字符串到缓冲区列表里
   * @param iterator LineIterator
   * @param bufList List
   * @param lines int
   */
  private static void readLines(LineIterator iterator, List<OrderByDto> bufList, int lines) {

    for(int i = 0; i < lines; i++) {
      if(!iterator.hasNext()) {
        break;
      }
      String line = iterator.nextLine();
      if("".equals(line.trim())){
        continue;
      }
      if(line.indexOf("INSERT INTO cf_trade_history")!=-1){
        start = true;
        total = total+100;
        continue;
      }

      if(!start){
        continue;
      }

      if(line.endsWith(",")||line.endsWith(";")){

      }else{
        System.out.println("不是逗号也不是分号结尾==> "+line);
        continue;
      }

      String value = line.substring(line.indexOf("(")+1,line.lastIndexOf(")"));
      value = value.replaceAll("'","");
      value = value.replaceAll("\\(","");
      value = value.replaceAll("\\)","");

      String[] split = value.split(",");
      String csv = "";
      for (int j=0;j<split.length;j++) {
        split[j] = split[j].trim();
        csv+=split[j]+",";
      }
      csv = csv.substring(0,csv.lastIndexOf(","));
      OrderByDto orderByDto = new OrderByDto();
      orderByDto.setSql(csv);
      //System.out.println(csv +" ==> " + line);
      orderByDto.setTime(Long.valueOf(split[6]));
      bufList.add(orderByDto);
    }
  }
  public static void sort(String fileSource,String fileOut, int blockSize) throws IOException {
    List<OrderByDto> list = new ArrayList<OrderByDto>(blockSize);
    Reader fC = null;
    fC = new FileReader(fileSource);
    LineIterator itC = IOUtils.lineIterator(fC);
    File file =new File(fileOut);
    //if file doesnt exists, then create it
    if(!file.exists()){
      file.createNewFile();
    }
    //true = append file
    FileWriter fileWritter = new FileWriter(fileOut,true);
    //orderid, marketid, accountid, marketcode, orderside, ordertype, ordertimestamp, lastupdated, lasttradetimestamp, timeinforce, clientorderid, status, lastmatchedorderid, lastmatchedorderid2, matchedid, matchedtype, quantity, remainingqty, price, triggerprice, triggerlimit, fees, feeinstrumentid, leg1_price, leg2_price, tradetype, base, counter, market_type, is_triggered, is_liquidation, source
    fileWritter.write("orderid,marketid,accountid,marketcode,orderside,ordertype,ordertimestamp,lastupdated,lasttradetimestamp,timeinforce,clientorderid,status,lastmatchedorderid,lastmatchedorderid2,matchedid,matchedtype,quantity,remainingqty,price,triggerprice,triggerlimit,fees,feeinstrumentid,leg1_price,leg2_price,tradetype,base,counter,market_type,is_triggered,is_liquidation,source\r\n");
    while(itC.hasNext()) {
      list.clear();
      readLines(itC, list, blockSize);
      //Collections.sort(list);
      for (OrderByDto orderByDto:list) {
        if(!"".equals(orderByDto.getSql())){
          fileWritter.write(orderByDto.getSql()+"\r\n");
        }
      }
    }
    fileWritter.close();
  }
  


  
  public static String formatSecond(long seconds) {  
    long h = seconds /(60*60);  
    StringBuffer sb = new StringBuffer();  
      
    sb.append(h+"小时");  
      
    seconds = seconds%(60*60);  
  
    long c = seconds /60;  
    sb.append(c+"分");  
  
    sb.append(seconds %60+"秒");  
      
    return sb.toString();  
  }  
  
  public static void main(String args[]) {  
    try {  
      //String fileName = "/Users/zenghuikang/workspace/coinflex/atguigu/flink/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/ESort.txt";

      //String fileName = "/Users/zenghuikang/Downloads/temp.sql";
      //int blockSize = 2;

      String fileName = "/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.sql";
      int blockSize = 1000000;

      long c1 = System.currentTimeMillis();  
      MergeSort2Csv.sort(fileName,"/Users/zenghuikang/Downloads/coinflex_cf_trade_history_2021-12-06.csv", blockSize);
      System.out.println("没有加最后一批的总行数==>"+(total-100L));
      //checkOrder(fileName + "_SORT");
      long c2 = (System.currentTimeMillis() - c1) / 1000;  
      System.out.println("耗时:"+formatSecond(c2));  
    } catch(IOException ex) {  
      ex.printStackTrace();  
    }  
  }  
  
} 