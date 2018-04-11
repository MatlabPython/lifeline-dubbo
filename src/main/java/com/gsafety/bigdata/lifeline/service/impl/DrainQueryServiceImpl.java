package com.gsafety.bigdata.lifeline.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.gsafety.bigdata.lifeline.dao.HBaseDao;
import com.gsafety.bigdata.lifeline.dao.PhoenixJdbcDao;
import com.gsafety.bigdata.lifeline.pojo.DrainParam;
import com.gsafety.bigdata.lifeline.pojo.GasParam;
import com.gsafety.bigdata.lifeline.service.DrainQueryService;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by zhangdm on 2017/12/6.
 */
public class DrainQueryServiceImpl implements DrainQueryService{
    private Logger logger = Logger.getLogger(DrainQueryServiceImpl.class);

    private HBaseDao hBaseDao;
    private PhoenixJdbcDao phoenixJdbcDao;

    @Override
    public String queryDrain(DrainParam drainParam) {
        List<Map<String, Object>> lists;
        String json = null;
        try {
            lists = phoenixJdbcDao.queryDrain(drainParam.getLocations(), drainParam.getTerninal(), drainParam.getSensor(), drainParam.getStarTime(),drainParam.getEndTime());
            json = JSONObject.toJSONString(lists);
        } catch (Exception e) {
            logger.error(e);
        }
        return json;
    }


    public void sethBaseDao(HBaseDao hBaseDao) {
        this.hBaseDao = hBaseDao;
    }

    public void setPhoenixJdbcDao(PhoenixJdbcDao phoenixJdbcDao) {
        this.phoenixJdbcDao = phoenixJdbcDao;
    }
}
