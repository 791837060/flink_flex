package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.OrderByDto;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MarketCodeAll {
  private MarketCodeAll() {
  }


  
  private static void checkOrder(String fileC) throws IOException {
    System.out.println("checkOrder");
    Reader fC = null;  
    try {
      fC = new FileReader(fileC);  
      LineIterator itC = IOUtils.lineIterator(fC);
      while(itC.hasNext()) {  
        String arketCode = itC.nextLine();
        String arketType = "";
        if (arketCode.contains("SWAP")) {
          arketType = "FUTURE";
          print1(arketCode,"SWAP");
        } else if (arketCode.contains("REPO")) {
          arketType = "REPO";
          print1(arketCode,"REPO");
        } else if (arketCode.contains("SPR")) {
          arketType = "SPREAD";
          print1(arketCode,"SPR");
        } else if (arketCode.contains("Rate")) {
          arketType = "INDEX";
          print1(arketCode,"Rate");
        } else if (arketCode.endsWith("USD")) {
          arketType = "SPOT";
          print1(arketCode,"USD");
        } else {
          arketType = "FUTURE";
          print1(arketCode,"ELSE");
        }


      }
    } finally {  
      IOUtils.closeQuietly(fC);
    }
  }

  public static void print1(String line,String type){

    try {
      if(!"".equals(line)){
        File file =new File(type+".txt");
        //if file doesnt exists, then create it
        if(!file.exists()){
          file.createNewFile();
        }
        //true = append file
        FileWriter fileWritter = new FileWriter(file.getName(),true);
        fileWritter.write(line+"\r\n");
        fileWritter.close();
        //System.out.println(tmpB);
      }
    } catch (Exception e){
      e.printStackTrace();
    }
  }

  private static OrderByDto toBean(String tmpB){
    String value = tmpB.substring(tmpB.indexOf("VALUES")+6,tmpB.lastIndexOf(")"));
    value = value.substring(value.indexOf("(")+1);
    value = value.replaceAll("'","");
    value = value.replaceAll("\\(","");
    value = value.replaceAll("\\)","");
    String[] split = value.split(",");
    for (int j=0;j<split.length;j++) {
      split[j] = split[j].trim();
    }
    OrderByDto orderByDto = new OrderByDto();
    orderByDto.setSql(tmpB);
    //System.out.println(split[6] +" " +line);
    orderByDto.setTime(Long.valueOf(split[6]));
    return orderByDto;
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
      String fileName = "/Users/zenghuikang/Downloads/marketcode_all";
      //String fileName = "/Users/zenghuikang/Downloads/temp.sql";
      //int blockSize = 2;
      int blockSize = 1000000;

      long c1 = System.currentTimeMillis();  
      //MergeSort2.sort(fileName,fileName + "_SORT", blockSize,true);
      checkOrder(fileName);
      long c2 = (System.currentTimeMillis() - c1) / 1000;  
      System.out.println("耗时:"+formatSecond(c2));  
    } catch(IOException ex) {  
      ex.printStackTrace();  
    }  
  }  
  
} 