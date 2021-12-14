package com.atguigu.hotitems_analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;


@Slf4j
public class AccountIdSql extends ProcessFunction<String, AccountMon> {
    private BigDecimal latestMarkPrice = BigDecimal.ZERO;

    @Override
    public void processElement(String line, Context ctx, Collector<AccountMon> out) throws Exception {
        try {

            String value = line.substring(line.indexOf("(")+1,line.lastIndexOf(")"));
            String[] split = value.split(",");
            for (int i=0;i<split.length;i++) {
                split[i] = split[i].trim();
            }

            AccountMon orderTick = new AccountMon();
            orderTick.setMod(Long.valueOf(split[2].trim())%100);
            orderTick.setValue(value);
            if(orderTick != null){
                out.collect(orderTick);
            }

        }catch (Exception ex){
            log.error("",ex);
            log.error("value ==> {}",line);
        }
    }
}
