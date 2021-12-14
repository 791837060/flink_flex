package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

/**
 * created by FlintZhou on 2021/10/14
 */
@Slf4j
public class TradeStatFlatMapFunction implements MapFunction<OrderTick, TradeFlatDto> {

    @Override
    public TradeFlatDto map(OrderTick orderTick) throws Exception {
        try {
            if (orderTick.getAccountId() != null && orderTick.getMarketCode() != null
                        && !"".equals(orderTick.getMarketCode())) {
                return new TradeFlatDto(orderTick);
            }
        } catch (Exception e) {
            //log.error("[TRADE_STAT] -> flat fun. tick:{}, e:{}", orderTick, e);
            log.error("",e);
        }
        return null;
    }
}
