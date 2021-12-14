package com.atguigu.hotitems_analysis;

import lombok.Data;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
public class OrderTick {
    public Long orderId;
    public Long matchId;
    public Long transTime;
    public Long clientOrderId;
    public String accountId;
    public String marketCode;
    public String marketType;
    public String action;
    public String side;
    public String orderType;
    public String matchType;
    public String base;
    public String counter;
    public BigDecimal price;
    public BigDecimal limitPrice;
    public BigDecimal triggerPrice;
    public BigDecimal matchedPrice;
    public BigDecimal markPrice;
    public BigDecimal qty;
    public BigDecimal matchedQty;
    public BigDecimal remainQty;
    public String timeInForce;
    public BigDecimal fees;
    public String feesCurrency;
    public Boolean isImplied;
    public Boolean isLiquidation;
    public Long impliedOrderId1;
    public Long impliedOrderId2;
    public BigDecimal leg1Price;
    public BigDecimal leg2Price;
    public String sourceType;
    public Timestamp processingTime;
    public String source;

    public OrderTick(Long transTime, String marketCode, String marketType, String side, String base, String counter, BigDecimal matchedPrice, BigDecimal leg1Price, BigDecimal leg2Price, BigDecimal matchedQty) {
        this.transTime = transTime;
        this.marketCode = marketCode;
        this.marketType = marketType;
        this.side = side;
        this.base = base;
        this.counter = counter;
        this.matchedPrice = matchedPrice;
        this.leg1Price = leg1Price;
        this.leg2Price = leg2Price;
        this.matchedQty = matchedQty;
    }

    public OrderTick(){
    }
}