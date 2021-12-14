package com.atguigu.hotitems_analysis.dto;

import lombok.Data;


/**
 * created by FlintZhou on 2021/10/14
 */
@Data
public class OrderByDto implements Comparable<OrderByDto>{

    public Long time;

    public String sql;


    @Override
    public int compareTo(OrderByDto o) {
        long id1 = this.getTime();
        long id2 = o.getTime();
        return id1 > id2 ? 1 : id1 == id2 ? 0 : -1;
    }

    @Override
    public String toString() {
        return sql;
    }
}
