package com.atguigu.hotitems_analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;


@Slf4j
public class CsvAccountIdSql extends ProcessFunction<String, AccountMon> {
    private BigDecimal latestMarkPrice = BigDecimal.ZERO;

    @Override
    public void processElement(String line, Context ctx, Collector<AccountMon> out) throws Exception {
        try {

            String value = "";
            String[] split = line.split(",");
            for (int i=0;i<32;i++) {
                if((i==3||i==4||i==5||i==7||i==8||i==9||i==11||i==17||i==24||i==25||i==26||i==27)&&!"NULL".equals(split[i])){
                    split[i] = "'"+split[i].trim()+"'";
                }else{
                    split[i] = split[i].trim();
                }
                value +=split[i]+",";
            }

            AccountMon orderTick = new AccountMon();
            orderTick.setMod(Long.valueOf(split[2].trim())%100);
            orderTick.setValue(value.substring(0,value.length()-1));
            /*
            orderid,marketid,accountid,marketcode,orderside,ordertype,ordertimestamp,lastupdated,lasttradetimestamp,timeinforce,
            clientorderid,status,lastmatchedorderid,lastmatchedorderid2,leg1_price,leg2_price,matchedid,matchedtype,quantity,remainingqty,
            price,triggerprice,triggerlimit,fees,feeinstrumentid,tradetype,base,counter,market_type,is_triggered,
            is_liquidation,source,price_test,quantity_test,tradetype_test,matchedtype_test

            304410084292095784,2001011000000,95253,BTC-USD-SWAP-LIN,BUY,MARKET,1618069198550,2021-04-10 15:39:58.639,2021-04-10 15:39:58.639,IOC,
            1136756662021,PARTIAL_FILL,304410084292095772,0,NULL,NULL,304410084292095785,TAKER,0.400000000,0.100000000,
            60283.500000000,NULL,NULL,7.234020000,USD,FUTURE,BTC,USD,NULL,false,
            NULL,NULL,NULL,NULL,TRADE,NULL*/
            if(orderTick != null){
                out.collect(orderTick);
            }

        }catch (Exception ex){
            log.error("",ex);
            log.error("value ==> {}",line);
        }
    }
}
