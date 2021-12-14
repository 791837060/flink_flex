package com.atguigu.hotitems_analysis;

import lombok.Data;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Data
public class AccountMon {
    public Long mod;
    public String value;

    public AccountMon(){
    }
}