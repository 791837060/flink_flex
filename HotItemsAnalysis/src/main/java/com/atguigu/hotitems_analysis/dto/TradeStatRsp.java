package com.atguigu.hotitems_analysis.dto;

import com.atguigu.hotitems_analysis.GsonUtil;
import com.atguigu.hotitems_analysis.pro.TradeStat1hPro;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

/**
 * created by FlintZhou on 2021/10/14
 */
@Data
public class TradeStatRsp {


    public long windowEndTime;
    public String dt;
    public String marketCode;
    public String marketType;
    public String orderSide;
    public String orderType;
    public String timeInforce;
    public String matchedType;

    public List<TradeFlatDto> tradeFlatDtoList;
    public BigDecimal volume;
    public BigDecimal currencyVolume;
    public String cycleType;
    public String accountId;
    public String source;

    public TradeStatRsp(String accountId,String source,long windowEndTime, String marketCode, String marketType, String orderSide, String orderType,
                        String timeInforce, String matchedType, BigDecimal volume, BigDecimal currencyVolume,List<TradeFlatDto> tradeFlatDtoList) {
        this.accountId = accountId;
        this.source = source;
        this.windowEndTime = windowEndTime;
        this.marketCode = marketCode;
        this.marketType = marketType;
        this.orderSide = orderSide;
        this.orderType = orderType;
        this.timeInforce = timeInforce;
        this.matchedType = matchedType;
        this.volume = volume;
        this.currencyVolume = currencyVolume;
        this.tradeFlatDtoList = tradeFlatDtoList;
    }

    public TradeStatRsp() {
    }

    public byte[] toJSONBytes() {
        return GsonUtil.GsonToJSONBytes(this);
    }

    @Override
    public String toString() {
        /*String valumes = "";
        for (TradeFlatDto tradeFlatDto:tradeFlatDtoList) {
            valumes+=""+tradeFlatDto.getMatchedQty()+",";
        }
        return valumes + " TradeStatRsp{" + "windowEndTime=" + windowEndTime + ", marketCode='" + marketCode + '\'' + ", marketType='" + marketType + '\'' + ", orderSide='" + orderSide + '\'' + ", orderType='" + orderType + '\'' + ", timeInforce='" + timeInforce + '\'' + ", matchedType='" + matchedType + '\'' + ", volume=" + volume + ", currencyVolume=" + currencyVolume + '}';*/

        //return "INSERT INTO public.trade_stat_1h (dt,market_code,order_side,order_type,time_inforce,matched_type,volume,currency_volume,market_type,cycle_type) VALUES ('"+dt+"','"+marketCode+"','"+orderSide+"','"+orderType+"','"+timeInforce+"','"+matchedType+"',"+volume+","+currencyVolume+",'"+marketType+"'" + (cycleType==null?","+cycleType+");\n":",'"+cycleType+"');\n");

        if(TradeStat1hPro.tradeStat1hPro){
            return "INSERT INTO public.trade_stat_1h (dt,market_code,order_side,order_type,time_inforce,matched_type,volume,currency_volume,market_type,cycle_type) VALUES ('"+dt+"','"+marketCode+"','"+orderSide+"','"+orderType+"','"+timeInforce+"','"+matchedType+"',"+volume.toPlainString()+","+currencyVolume.toPlainString()+",'"+marketType+"'" + (cycleType==null?(","+cycleType+");"):(",'"+cycleType+"');"));
        }else{
            return "INSERT INTO public.account_trade_1h (account_id,dt,market_code,order_side,order_type,time_inforce,matched_type,volume,currency_volume,market_type,\"source\",cycle_type) VALUES ("+accountId+",'"+dt+"','"+marketCode+"','"+orderSide+"','"+orderType+"','"+timeInforce+"','"+matchedType+"',"+volume.toPlainString()+","+currencyVolume.toPlainString()+",'"+marketType+"'"+","+source + (cycleType==null?(","+cycleType+");"):(",'"+cycleType+"');"));
        }
    }
}
