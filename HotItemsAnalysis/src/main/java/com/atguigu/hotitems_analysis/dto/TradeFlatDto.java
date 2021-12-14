package com.atguigu.hotitems_analysis.dto;

import com.atguigu.hotitems_analysis.OrderTick;
import lombok.Data;

import java.math.BigDecimal;

/**
 * created by FlintZhou on 2021/10/14
 */
@Data
public class TradeFlatDto {

    public Long transTime;

    public String marketCode; //  -- BTC-USD,
    public String marketType; //   -- TRADE
    public String orderSide; //   -- BUY/SELL
    public String orderType; //   -- LIMIT
    public String timeInforce; // -- MAKER_ONLY
    public String matchedType; // -- MAKET/TAKER
    public String accountId; // -- MAKET/TAKER
    public String source; // -- MAKET/TAKER

    public BigDecimal matchedPrice;
    public BigDecimal matchedQty;
    public BigDecimal leg1Price;
    public BigDecimal leg2Price;
    public BigDecimal fees;
    public String feesCurrency;

    public TradeFlatDto(OrderTick orderTick) {
        this.transTime = orderTick.getTransTime();
        this.marketCode = orderTick.getMarketCode();
        this.marketType = orderTick.getMarketType();
        this.orderSide = orderTick.getSide();
        this.orderType = orderTick.getOrderType();
        this.timeInforce = orderTick.getTimeInForce();
        this.matchedType = orderTick.getMatchType();

        this.matchedPrice = orderTick.getMatchedPrice();
        this.matchedQty = orderTick.getMatchedQty();
        this.leg1Price = orderTick.getLeg1Price();
        this.leg2Price = orderTick.getLeg2Price();
        this.fees = orderTick.getFees();
        this.feesCurrency = orderTick.getFeesCurrency();

        this.accountId  = orderTick.getAccountId();
        this.source = orderTick.getSource();
    }

    public TradeFlatDto() {
    }
}
