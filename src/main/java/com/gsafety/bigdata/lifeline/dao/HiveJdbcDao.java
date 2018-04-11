package com.gsafety.bigdata.lifeline.dao;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangdm on 2017/9/7.
 */
public class HiveJdbcDao {
    private JdbcTemplate hiveJdbcTemplate;

//    private static final String BRIDGE_TABLE="";
//    private static final String WATER_TABLE="";
//    private static final String GAS_TABLE="";

    private List<Map<String, Object>> query(String locations, String terninal, String sensor, String starTime, String endTime, String table, String dataType){

        return null;
    }






















    public void setHiveJdbcTemplate(JdbcTemplate hiveJdbcTemplate) {
        this.hiveJdbcTemplate = hiveJdbcTemplate;
    }

}
