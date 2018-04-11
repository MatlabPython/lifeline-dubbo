package com.gsafety.bigdata.lifeline.service.impl;

import java.util.List;
import java.util.Map;

import com.gsafety.bigdata.lifeline.dao.PhoenixJdbcDao;
import com.gsafety.bigdata.lifeline.pojo.ResultMessage;
import org.apache.log4j.Logger;
import com.alibaba.fastjson.JSONObject;
import com.gsafety.bigdata.lifeline.dao.HBaseDao;
import com.gsafety.bigdata.lifeline.pojo.WaterParam;
import com.gsafety.bigdata.lifeline.service.WaterQueryService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class WaterQueryServiceImpl implements WaterQueryService {
	
	private HBaseDao  hBaseDao;
	private PhoenixJdbcDao phoenixJdbcDao;
	private Logger logger =Logger.getLogger(WaterQueryServiceImpl.class);
	
	public void sethBaseDao(HBaseDao hBaseDao) {
		this.hBaseDao = hBaseDao;
	}

	public void setPhoenixJdbcDao(PhoenixJdbcDao phoenixJdbcDao) {
		this.phoenixJdbcDao = phoenixJdbcDao;
	}

	public String queryWater(WaterParam waterParam) {
		List<Map<String, Object>> lists ;
		String json = null;
		try {
			lists = phoenixJdbcDao.queryWater(waterParam.getLocations(), waterParam.getTerninal(), waterParam.getSensor(), waterParam.getStarTime(),waterParam.getEndTime(), waterParam.getSample());
			json =JSONObject.toJSONString(lists);
		} catch (Exception e) {
			logger.error(e);
		}
		return json;
	}

	public String queryWaterFlow(WaterParam waterParam) {
		List<Map<String, Object>> lists;
		String json = null;
		try {
			lists = phoenixJdbcDao.queryWaterFlow(waterParam.getLocations(), waterParam.getTerninal(), waterParam.getSensor(), waterParam.getStarTime(),waterParam.getEndTime());
			json =JSONObject.toJSONString(lists);
		} catch (Exception e) {
			logger.error(e);
		}
				
		return json;
	}

public ResultMessage queryWaterV2(WaterParam waterParam) {
	List<Map<String, Object>> lists ;
	if (phoenixJdbcDao==null){
		return ResultMessage.getMessageFromCode("102");
	}
	try {
		lists = phoenixJdbcDao.queryWater(waterParam.getLocations(), waterParam.getTerninal(), waterParam.getSensor(), waterParam.getStarTime(),waterParam.getEndTime(), waterParam.getSample());
	} catch (Exception e) {
		logger.error(e);
		return ResultMessage.getMessageFromCode("201");
	}
	if(lists==null||lists.size()<1){
		return ResultMessage.getMessageFromCode("301");
	}
	String json=toJson(lists);
	if ("401".equals(json)){
		return ResultMessage.getMessageFromCode("401");
	}
	return new ResultMessage("000",json);
}

public ResultMessage queryWaterFlowV2(WaterParam waterParam) {
	List<Map<String, Object>> lists;
	if (phoenixJdbcDao==null){
		return ResultMessage.getMessageFromCode("102");
	}
	try {
		lists = phoenixJdbcDao.queryWaterFlow(waterParam.getLocations(), waterParam.getTerninal(), waterParam.getSensor(), waterParam.getStarTime(),waterParam.getEndTime());
	} catch (Exception e) {
		logger.error(e);
		return ResultMessage.getMessageFromCode("201");
	}
	if(lists==null||lists.size()<1){
		return ResultMessage.getMessageFromCode("301");
	}
	String json=toJson(lists);
	if ("401".equals(json)){
		return ResultMessage.getMessageFromCode("401");
	}
	return new ResultMessage("000",json);
}
	private  String toJson(Object o ){
		try {
			String result = JSONObject.toJSONString(o);
			return result;
		}catch (Exception e) {
			logger.info(e);
			return "401";
		}
	}

	public static void main(String[] args) {
		ApplicationContext ac = new ClassPathXmlApplicationContext("phoenix-jdbc.xml");
		PhoenixJdbcDao phoenixJdbcdao = ac.getBean("phoenixJdbcDao", PhoenixJdbcDao.class);
		WaterQueryServiceImpl wi = new WaterQueryServiceImpl();
		wi.setPhoenixJdbcDao(phoenixJdbcdao);
		String localtion = "00000000";
		String terninal = "000212";
		String sensor = "1_0";
		String startTime = "2017-06-20 00:50:00";
		String endTime = "2017-09-26 23:50:00";
		WaterParam waterParam =new  WaterParam(localtion,terninal,sensor,startTime,endTime,"");

		ResultMessage resultMessage = wi.queryWaterFlowV2(waterParam);
		System.out.println(resultMessage);
	}
/*	public static void main(String[] args) {
		String localtion = "00000000";
		String terninal = "000212";
		String sensor = "1_0";
		String startTime = "2017-06-20 00:50:00";
		String endTime = "2017-06-26 23:50:00";
		
		WaterParam waterParam =new  WaterParam(localtion,terninal,sensor,startTime,endTime);
		WaterQueryServiceImpl  impl = new WaterQueryServiceImpl();
		impl.sethBaseDao(new  HBaseDao());  
		impl.queryWater(waterParam);
	}*/
}
