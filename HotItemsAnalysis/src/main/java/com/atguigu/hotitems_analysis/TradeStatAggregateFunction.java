package com.atguigu.hotitems_analysis;

import com.atguigu.hotitems_analysis.dto.TradeFlatDto;
import com.atguigu.hotitems_analysis.dto.TradeStatRsp;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Optional;

import static com.atguigu.hotitems_analysis.OhlcConstant.REPO;
import static com.atguigu.hotitems_analysis.OhlcConstant.SPREAD;


/**
 * created by FlintZhou on 2021/10/14
 */
public class TradeStatAggregateFunction implements AggregateFunction<TradeFlatDto, TradeStatRsp, TradeStatRsp> {
    @Override
    public TradeStatRsp createAccumulator() {
        return new TradeStatRsp(null,null,0, null, null, null, null,
                null, null, BigDecimal.ZERO, BigDecimal.ZERO,new ArrayList<TradeFlatDto>());
    }

    @Override
    public TradeStatRsp add(TradeFlatDto in, TradeStatRsp accumulator) {
        accumulator.setMarketCode(in.getMarketCode());
        if (accumulator.getMarketCode().contains("SWAP")) {
            accumulator.setCycleType("SWAP");
        }

        /*
                1,marketcode  2,market_type/tradetype 3,ycleType

                %SWAP%  FUTURE SWAP
                %REPO%  REPO null
                %SPR%   SPREAD null
                %Rate%  INDEX null
                %USD    SPOT null
                其它情况  FUTURE null
        */
        if (accumulator.getMarketCode().contains("SWAP")) {
            in.setMarketType("FUTURE");
        } else if (accumulator.getMarketCode().contains("REPO")) {
            in.setMarketType("REPO");
        } else if (accumulator.getMarketCode().contains("SPR")) {
            in.setMarketType("SPREAD");
        } else if (accumulator.getMarketCode().contains("Rate")) {
            in.setMarketType("INDEX");
        } else if (accumulator.getMarketCode().endsWith("USD")) {
            in.setMarketType("SPOT");
        } else {
            in.setMarketType("FUTURE");
        }

        accumulator.setMarketType(in.getMarketType());
        accumulator.setOrderSide(in.getOrderSide());
        accumulator.setOrderType(in.getOrderType());
        accumulator.setTimeInforce(in.getTimeInforce());
        accumulator.setMatchedType(in.getMatchedType());
        accumulator.setCurrencyVolume(accumulator.getCurrencyVolume().add(Optional.ofNullable(in.getMatchedQty()).orElse(BigDecimal.ZERO)));

        accumulator.setAccountId(in.getAccountId());
        accumulator.setSource(in.getSource());

        if (REPO.equals(in.getMarketType()) || SPREAD.equals(in.getMarketType())) {
            accumulator.setVolume(accumulator.getVolume()
                    .add((safeDec(in.getLeg1Price())
                            .add(safeDec(in.getLeg2Price()))).abs()
                            .multiply(new BigDecimal("0.5"))
                            .multiply(safeDec(in.getMatchedQty()))));
            accumulator.getTradeFlatDtoList().add(in);
        } else {
            accumulator.setVolume(accumulator.getVolume().add(
                    Optional.ofNullable(in.getMatchedPrice()).orElse(BigDecimal.ZERO).abs()
                            .multiply(Optional.ofNullable(in.getMatchedQty()).orElse(BigDecimal.ZERO))));
            accumulator.getTradeFlatDtoList().add(in);
        }

        /*if(1606834800000L>tradeFlatDto.getTransTime()&& tradeFlatDto.getTransTime()>1606831200000L
        &&tradeFlatDto.getMarketCode().equals("BTC-USD-SWAP-LIN") && "SELL".equals(tradeFlatDto.getOrderSide()) &&
                   "LIMIT".equals(tradeFlatDto.getOrderType()) && "IOC".equals(tradeFlatDto.getTimeInforce()) && "TAKER".equals(tradeFlatDto.getMatchedType())){
            try {
                File file =new File("count.txt");
                //if file doesnt exists, then create it
                if(!file.exists()){
                    file.createNewFile();
                }
                //true = append file
                FileWriter fileWritter = new FileWriter(file.getName(),false);
                fileWritter.write(tradeFlatDto.getMatchedPrice() + " " + tradeFlatDto.getMatchedQty() +"\r\n");
                fileWritter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

        return accumulator;
    }

    private BigDecimal safeDec(BigDecimal num) {
        return num == null ? BigDecimal.ZERO : num;
    }

    @Override
    public TradeStatRsp getResult(TradeStatRsp acc) {
        return acc;
    }

    /**
     * 一般我们用不到
     * @param acc1
     * @return
     */
    @Override
    public TradeStatRsp merge(TradeStatRsp acc1, TradeStatRsp acc2) {
        return null;
    }
}
