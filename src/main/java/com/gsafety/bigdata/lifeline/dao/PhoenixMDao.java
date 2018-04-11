package com.gsafety.bigdata.lifeline.dao;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/9/7.
 */
public interface PhoenixMDao {
//    public List<Map> selectLimit(@Param(value = "table") String table, @Param(value = "limitNum") int limitNum);
    public List<Map> selectLimit(String table,int limitNum);
//    public List<Map> selectLimit(@Param(value = "table") String table);
}
