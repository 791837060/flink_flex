package com.atguigu.hotitems_analysis;

import org.apache.flink.util.OutputTag;

public interface OhlcConstant {
    String JOBOHLC = "ohlc";
    String JOBMARKPRICE = "markprice";
    String JOBPRICING = "pricing";

    String candle_table_1m = "candle_table_1m";
    String candle_table_3m = "candle_table_3m";
    String candle_table_5m = "candle_table_5m";
    String candle_table_10m = "candle_table_10m";
    String candle_table_15m = "candle_table_15m";
    String candle_table_30m = "candle_table_30m";
    String candle_table_1h = "candle_table_1h";
    String candle_table_2h = "candle_table_2h";
    String candle_table_4h = "candle_table_4h";
    String candle_table_6h = "candle_table_6h";
    String candle_table_12h = "candle_table_12h";
    String candle_table_24h = "candle_table_24h";
    String candle_table_1d = "candle_table_1d";
    String candle_table_1w = "candle_table_1w";

    OutputTag<OrderTick> tickerOutPutTag = new OutputTag<OrderTick>("ticker-output"){};

    String REPO = "REPO";
    String SPREAD = "SPREAD";
    String SPOT = "SPOT";
    String FUTURE = "FUTURE";

    String TICKER = "TICKER";
    String MARKPRICE = "MARKPRICE";
    String VOLUME = "VOLUME";

    String BUY = "BUY";
    String SELL = "SELL";

    String BASE = "BASE";
    String COUNTER = "COUNTER";

    String PRICING = "PRICING";
    String ORDERTICK = "ORDERTICK";

    String pgDriver = "org.postgresql.Driver";
    String propertiesPath = "/opt/flink/conf/application.properties";
}
