package com.atguigu.hotitems_analysis;

import java.io.File;
import java.io.FileWriter;

public class CfTradeHisSql {
    public static void main(String[] argw){
        String sql = "select 'delete from cf_trade_history_1 as ta where ta.ctid <> ( select max(tb.ctid) from cf_trade_history_1 as tb where ' || orderid || ' = tb.orderid and '|| matchedid|| ' = tb.matchedid) and '|| orderid|| ' = ta.orderid and '|| matchedid|| ' = ta.matchedid;' from cf_trade_history_1 cth group by orderid,matchedid having count(1) > 1;";
        String sql3 = "ALTER TABLE public.cf_trade_history_1 ADD CONSTRAINT cf_trade_history_1_pk PRIMARY KEY (matchedid,orderid);";
        for (int i=0;i<100;i++) {
            String table = "cf_trade_history_"+i;
            String sql2 = sql.replaceAll("cf_trade_history_1",table);

            String sql4 = sql3.replaceAll("cf_trade_history_1",table);
            //System.out.println(sql2);
            System.out.println(sql4);
            //System.out.println("");
        }

    }

    /*public static void main(String[] argw){
        String sql = "ALTER TABLE public.cf_trade_history_1 ADD CONSTRAINT cf_trade_history_1_pk PRIMARY KEY (matchedid,orderid);";

        for (int i=0;i<100;i++) {
            String table = "cf_trade_history_"+i;
            String sql2 = sql.replaceAll("cf_trade_history_1",table);
            //System.out.println(sql2);
            try{
                File file =new File("/Volumes/Seagate Expansion Drive/Download/temp.txt");
                //File file =new File("temp.txt");
                //if file doesnt exists, then create it
                if(!file.exists()){
                    file.createNewFile();
                }
                //true = append file
                FileWriter fileWritter = new FileWriter(file.getName(),true);
                fileWritter.write(sql2+"\r\n");
                fileWritter.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

    }*/



}
