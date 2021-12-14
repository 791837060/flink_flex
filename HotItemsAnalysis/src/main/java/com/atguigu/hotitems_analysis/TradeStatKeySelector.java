package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * created by FlintZhou on 2021/10/14
 */
public class TradeStatKeySelector implements
        KeySelector<TradeFlatDto, Tuple5<String, String, String, String, String>> {

    @Override
    public Tuple5<String, String, String, String, String> getKey(TradeFlatDto trade) {
        return new Tuple5<>(trade.getMarketCode(), trade.getOrderSide(),
                trade.getOrderType(), trade.getTimeInforce(), trade.getMatchedType());
    }
}
