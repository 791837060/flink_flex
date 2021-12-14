package com.atguigu.hotitems_analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;


@Slf4j
public class OrderTickProcessFunctionCsv extends ProcessFunction<String, OrderTick> {
    private BigDecimal latestMarkPrice = BigDecimal.ZERO;

    @Override
    public void processElement(String value, Context ctx, Collector<OrderTick> out) throws Exception {
        try {
            // 160075464518799539,2001011000000,809973,'BTC-USD-SWAP-LIN','SELL','LIMIT',1596391046740,'2020-08-02 17:57:26.777+00:00','2020-08-02 17:57:26.777+00:00','IOC',
            // 0,'FILLED',160075464518799537,0,160075464518799540,'TAKER',0.407000000,0E-9,11290.500000000,NULL,
            // NULL,7.432700000,'FLEX',NULL,NULL,'TRADE',NULL,NULL,NULL,false,
            // NULL,NULL);

            // orderid,marketid,accountid,marketcode,orderside,ordertype,ordertimestamp,lastupdated,lasttradetimestamp,timeinforce,
            // clientorderid,status,lastmatchedorderid,lastmatchedorderid2,matchedid,matchedtype,quantity,remainingqty,price,triggerprice,
            // triggerlimit,fees,feeinstrumentid,leg1_price,leg2_price,tradetype,base,counter,market_type,is_triggered,
            // is_liquidation,source

            // 160063394735872921,2001011000000,541799,BTC-USD-SWAP-LIN,BUY,LIMIT,1594839892585,2020-07-15 19:04:52.605+00:00,2020-07-15 19:04:52.605+00:00,MAKER_ONLY_REPRICE,
            // 1591,PARTIAL_FILL,160063394735872926,0,160063394735872927,MAKER,0.274000000,0.726000000,9180.500000000,NULL,
            // NULL,-1.812900000,FLEX,NULL,NULL,TRADE,NULL,NULL,NULL,false,
            // NULL,NULL

            String[] split = value.split(",");
            for (int i=0;i<split.length;i++) {
                split[i] = split[i].trim();
            }

            OrderTick orderTick = new OrderTick();
            orderTick.setTransTime(Long.valueOf(split[6]));
            orderTick.setMarketCode(split[3]);
            orderTick.setMarketType(split[25]);
            orderTick.setSide(split[4]);
            orderTick.setOrderType(split[5]);
            orderTick.setTimeInForce(split[9]);
            orderTick.setMatchType(split[15]);
            orderTick.setMatchedPrice("NULL".equals(split[18])?BigDecimal.ZERO : new BigDecimal(split[18]));
            orderTick.setMatchedQty("NULL".equals(split[16])?BigDecimal.ZERO : new BigDecimal(split[16]));
            orderTick.setLeg1Price("NULL".equals(split[23])?BigDecimal.ZERO : new BigDecimal(split[23]));
            orderTick.setLeg2Price("NULL".equals(split[24])?BigDecimal.ZERO : new BigDecimal(split[24]));
            orderTick.setFees("NULL".equals(split[21])?BigDecimal.ZERO : new BigDecimal(split[21]));
            orderTick.setFeesCurrency(split[22]);

            orderTick.setSourceType(split[15]);
            orderTick.setAccountId(split[2]);

            orderTick.setSource("NULL".equals(split[31])?"0":split[31]);


            if(orderTick != null&&orderTick.getTransTime()<1638662400000L){
                out.collect(orderTick);
                //log.info("MessageMarketCode:{}, matchedPrice:{}, matchedQty:{}, source:{}", orderTick.getMarketCode(), orderTick.getMatchedPrice(), orderTick.getMatchedQty(), orderTick.getSourceType());
            }

        }catch (Exception ex){
            //log.error("Parsing OrderTick exception: {}, msg: {}",ex, value);
            log.error("",ex);
            log.error("value ==> {}",value);
        }
    }
}
