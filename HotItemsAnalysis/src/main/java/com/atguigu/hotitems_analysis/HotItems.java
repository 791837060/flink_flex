package com.atguigu.hotitems_analysis;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: UserBehaviorAnalysis
 * Package: com.atguigu.hotitems_analysis
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/14 15:16
 */

import com.atguigu.hotitems_analysis.beans.ItemViewCount;
import com.atguigu.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

/**
 * @ClassName: HotItems
 * @Description:
 * @Author: wushengran on 2020/11/14 15:16
 * @Version: 1.0
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = env.readTextFile("/Users/zenghuikang/workspace/coinflex/尚硅谷大数据技术之Flink（Java版）/4.代码/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));


        // 3. 转换为POJO，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))    // 过滤pv行为
                .keyBy("itemId")    // 按商品ID分组
                .timeWindow(Time.hours(1), Time.minutes(5))    // 开滑窗
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        /*前三种类型的 Window Fucntion 按照计算原理的不同可以分为两大类：
        一类是增量聚合函数：对应有 ReduceFunction、AggregateFunction；
        另一类是全量窗口函数，对应有 ProcessWindowFunction（还有 WindowFunction）。增量聚合函数计算性能较高，占用存储空间少，主要因为基于中间状态的计算结果，
        窗口中只维护中间结果状态值，不需要缓存原始数据。而全量窗口函数使用的代价相对较高，   性能比较弱，主要因为此时算子需要对所有属于该窗口的接入数据进行缓存，然后等到窗口触发的时候，
        对所有的原始数据进行汇总计算。
————————————————
        版权声明：本文为CSDN博主「旧时光中的旅人」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
        原文链接：https://blog.csdn.net/weixin_46266718/article/details/109520232*/

        // 5. 收集同一窗口的所有商品count数据，排序输出top n
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")    // 按照窗口分组
                .process(new TopNHotItems(5));   // 用自定义处理函数排序取前5

        resultStream.print();

        env.execute("hot items analysis");
    }

    // 实现自定义增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            if(value.getItemId().longValue()==2338453){
                System.out.println("ThreadId:"+Thread.currentThread().getId()+",商品id为"+value.getItemId().longValue()+ "的pv累加器:"+(accumulator)+",用户行为的时间戳:"+new Timestamp(value.getTimestamp()*1000 - 1));
            }
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            if(itemId.longValue()==2338453){
                System.out.println("ThreadId:"+Thread.currentThread().getId()+",商品id为"+itemId.longValue()+ "的count:"+count+",windowEnd:"+new Timestamp(windowEnd - 1));
            }
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    // 实现自定义KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String>{
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(value);
            //当前窗口时间戳一样，重复注册没问题
            ctx.timerService().registerEventTimeTimer( value.getWindowEnd() + 1 );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
