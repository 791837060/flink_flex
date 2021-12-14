package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * created by FlintZhou on 2021/10/14
 */
public class AccountTradeStatKeySelector implements
        KeySelector<TradeFlatDto, Tuple7<String, String,String, String, String, String, String>> {

    @Override
    public Tuple7<String, String,String, String, String, String, String> getKey(TradeFlatDto trade) {
        return new Tuple7<>(trade.getAccountId(),trade.getMarketCode(), trade.getOrderSide(),trade.getOrderType(), trade.getTimeInforce(), trade.getMatchedType(),trade.getSource());
    }
}
