package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.OrderByDto;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MergeSort3 {
  private MergeSort3() {
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
      String value = line.substring(line.indexOf("VALUES")+6,line.lastIndexOf(")"));
      value = value.substring(value.indexOf("(")+1);
      value = value.replaceAll("'","");
      value = value.replaceAll("\\(","");
      value = value.replaceAll("\\)","");
      String[] split = value.split(",");
      for (int j=0;j<split.length;j++) {
        split[j] = split[j].trim();
      }
      OrderByDto orderByDto = new OrderByDto();
      orderByDto.setSql(line);
      //System.out.println(split[6] +" " +line);
      orderByDto.setTime(Long.valueOf(split[6]));
      bufList.add(orderByDto);
    }
  }
  public static void sort(String fileSource,String fileOut, int blockSize,boolean check) throws IOException {
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
    while(itC.hasNext()) {
      list.clear();
      readLines(itC, list, blockSize);
      Collections.sort(list);
      for (OrderByDto orderByDto:list) {
        if(!"".equals(orderByDto.getSql())){
          fileWritter.write(orderByDto.getSql()+"\r\n");
        }
      }
    }
    fileWritter.close();
    if(check) {
      checkOrder(fileOut);
    }  
  }
  
  private static void checkOrder(String fileC) throws IOException {
    System.out.println("checkOrder");
    Reader fC = null;  
    try {
      fC = new FileReader(fileC);  
      OrderByDto orderByDtoA=new OrderByDto();
      OrderByDto orderByDtoB=new OrderByDto();
      LineIterator itC = IOUtils.lineIterator(fC);
      while(itC.hasNext()) {  
        String tmpB = itC.nextLine();
        orderByDtoB = toBean(tmpB);
        if(orderByDtoB.getTime()!=0&&orderByDtoA.getTime()!=0&&orderByDtoB.getTime()<orderByDtoA.getTime()) {
          if(!"".equals(tmpB)){
            File file =new File("no-order.txt");
            //if file doesnt exists, then create it
            if(!file.exists()){
              file.createNewFile();
            }
            //true = append file
            FileWriter fileWritter = new FileWriter(file.getName(),true);
            fileWritter.write(tmpB+"\r\n");
            fileWritter.close();
            //System.out.println(tmpB);
          }
        }
        orderByDtoA.setTime(orderByDtoB.getTime());
        orderByDtoA.setSql(orderByDtoB.getSql());
      }  
    } finally {  
      IOUtils.closeQuietly(fC);
    }
  }


  private static void removeDuple(String fileC) throws IOException {
    System.out.println("插入乱序");
    Reader fC = null;
    Writer fTemp = null;
    File tempFile = File.createTempFile("wjw", null,new File("/Users/zenghuikang/workspace/coinflex/atguigu/flink/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/"));
    try {
      fC = new FileReader(fileC);
      fTemp = new BufferedWriter(new FileWriter(tempFile));

      String tmpA = "";
      String tmpB = "";
      LineIterator itC = IOUtils.lineIterator(fC);
      while(itC.hasNext()) {
        tmpB = itC.nextLine();
        if(tmpB.compareTo(tmpA) != 0) {
          IOUtils.write(tmpB + "\n", fTemp);
          tmpA = tmpB;
        }
      }
    } finally {
      IOUtils.closeQuietly(fTemp);
      IOUtils.closeQuietly(fC);
    }

    File cFile = new File(fileC);
    if(cFile.delete()) {
      if(tempFile.renameTo(cFile)) {
        tempFile.delete();
      }
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
      String fileName = "/Users/zenghuikang/Downloads/cf_trade_history_001.sql";
      //String fileName = "/Users/zenghuikang/Downloads/temp.sql";
      //int blockSize = 2;
      int blockSize = 1000000;

      long c1 = System.currentTimeMillis();  
      MergeSort3.sort(fileName,fileName + "_SORT", blockSize,true);
      //checkOrder(fileName + "_SORT");
      long c2 = (System.currentTimeMillis() - c1) / 1000;  
      System.out.println("耗时:"+formatSecond(c2));  
    } catch(IOException ex) {  
      ex.printStackTrace();  
    }  
  }  
  
} 