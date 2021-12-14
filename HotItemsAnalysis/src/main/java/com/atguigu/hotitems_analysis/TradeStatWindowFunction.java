package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import com.atguigu.hotitems_analysis.dto.TradeStatRsp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * created by FlintZhou on 2021/10/14
 */
@Slf4j
public class TradeStatWindowFunction implements WindowFunction<TradeStatRsp, TradeStatRsp,
        Tuple5<String, String, String, String, String>, TimeWindow> {

    @Override
    public void apply(Tuple5<String, String, String, String, String> tuple5,
                      TimeWindow window, Iterable<TradeStatRsp> input, Collector<TradeStatRsp> collector) throws Exception {
        try {
            //System.out.println(tuple5.getField(0).toString() +","+tuple5.getField(1).toString()+","+tuple5.getField(2).toString()+","+tuple5.getField(3).toString()+","+tuple5.getField(4).toString());
            TradeStatRsp tradeStatRsp = input.iterator().next();
            /*String str = "";
            for(TradeFlatDto dto : tradeStatRsp.getTradeFlatDtoList()){
                str += new Timestamp(dto.getTransTime()).toString()+",";
            }*/
            //System.out.println(tradeStatRsp.getMarketCode()+","+ tradeStatRsp.getOrderSide()+","+tradeStatRsp.getOrderType()+","+ tradeStatRsp.getTimeInforce()+","+tradeStatRsp.getMatchedType() +" start= "+new Timestamp( window.getStart() ).toString() +", end= "+new Timestamp( window.getEnd() ).toString() +",list = "+str);
            tradeStatRsp.setWindowEndTime(window.getEnd());
            tradeStatRsp.setDt(new Timestamp( window.getStart() ).toString());
            //System.out.println("select sum(quantity*abs(price)) from cf_trade_history cth where ordertimestamp > "+window.getStart()+" and ordertimestamp <="+window.getEnd()+" and marketcode ='"+tradeStatRsp.getMarketCode()+"' and orderSide='"+tradeStatRsp.getOrderSide()+"' and orderType='"+tradeStatRsp.getOrderType()+"' and timeInforce='"+tradeStatRsp.getTimeInforce()+"' and matchedType='"+tradeStatRsp.getMatchedType()+"';");
            collector.collect(tradeStatRsp);
        } catch (Exception e) {
            //log.error("[TRADE_STAT] -> window e:{}", e);
            log.error("", e);
        }
    }
}
