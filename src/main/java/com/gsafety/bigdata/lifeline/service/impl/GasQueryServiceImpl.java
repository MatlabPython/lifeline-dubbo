package com.gsafety.bigdata.lifeline.service.impl;

import java.util.List;
import java.util.Map;

import com.gsafety.bigdata.lifeline.dao.PhoenixJdbcDao;
import com.gsafety.bigdata.lifeline.pojo.ResultMessage;
import org.apache.log4j.Logger;
import com.alibaba.fastjson.JSONObject;
import com.gsafety.bigdata.lifeline.dao.HBaseDao;
import com.gsafety.bigdata.lifeline.pojo.GasParam;
import com.gsafety.bigdata.lifeline.service.GasQueryService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class GasQueryServiceImpl  implements GasQueryService{

	private HBaseDao  hBaseDao;
	private PhoenixJdbcDao phoenixJdbcDao;

	private Logger logger =Logger.getLogger(GasQueryServiceImpl.class);
	
	public void sethBaseDao(HBaseDao hBaseDao) {
		this.hBaseDao = hBaseDao;
	}

	public void setPhoenixJdbcDao(PhoenixJdbcDao phoenixJdbcDao) {
		this.phoenixJdbcDao = phoenixJdbcDao;
	}

	public String queryGas(GasParam gasParam) {
		List<Map<String, Object>> lists;
		String json = null;
		try {
			lists = phoenixJdbcDao.queryGas(gasParam.getLocations(), gasParam.getTerninal(), gasParam.getSensor(), gasParam.getStarTime(),gasParam.getEndTime());
			json =JSONObject.toJSONString(lists);
		} catch (Exception e) {
			logger.error(e);
		}
		return json;
	}

	public ResultMessage queryGasV2(GasParam gasParam) {
		List<Map<String, Object>> lists;
		if(phoenixJdbcDao==null){
			System.out.println("null");
			logger.info("phoenixJdbcDao为空");
			return ResultMessage.getMessageFromCode("101");
		}
		try {
			lists = phoenixJdbcDao.queryGas(gasParam.getLocations(), gasParam.getTerninal(), gasParam.getSensor(), gasParam.getStarTime(),gasParam.getEndTime());
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
		GasQueryServiceImpl gi = new GasQueryServiceImpl();
		gi.setPhoenixJdbcDao(phoenixJdbcdao);
		List<Map<String, Object>> mapList ;
		for (int i=0;i<100;i++){
			String localtion = "00000000";
			String terninal = "9C03026039";
			String sensor = "0_0";
			String endTime = "2017-09-30 13:48:29";
			String startTime = "2017-07-31 13:58:29";
			GasParam gasPara = new GasParam(localtion, terninal, sensor, startTime, endTime);
			ResultMessage resultMessage = gi.queryGasV2(gasPara);
			System.out.println(resultMessage);
		}

////		String localtion = "00000000";
////		String terninal = "1001";
////		String sensor = "1_2";
////		String startTime = "2017-06-26 00:50:00";
////		String endTime = "2017-06-26 23:50:00";
////
////		GasParam gasParam =new GasParam("00000000","1001","1_2","2017-06-22 00:00:00","2017-06-28 23:59:00");
//		GasQueryServiceImpl gasService =new  GasQueryServiceImpl();
////
////
//		gasService.sethBaseDao(new  HBaseDao());
//		gasService.setPhoenixJdbcDao(new PhoenixJdbcDao());
////
////
////		String res =impl.queryGas(gasParam);
////		System.out.println(res);
////		String localtion = "00000000";
////		String terninal = "9C03026039";
////		String sensor = "0_0";
////		String endTime = "2017-08-30 13:48:29";
////		String startTime = "2017-07-31 13:58:29";
//		String localtion = "00000000";
//		String terninal = "9C03026039";
//		String sensor = "0_0";
//		String endTime = "2017-09-30 13:48:29";
//		String startTime = "2017-07-31 13:58:29";
//		GasParam gasPara = new GasParam(localtion, terninal, sensor, startTime, endTime);
////		String s = gasService.queryGas(gasPara);
//		System.out.println(gasService.queryGasV2(gasPara));
////		GasParam gasPara = new GasParam(localtion, terninal, sensor, startTime, endTime);
////		GasQueryServiceImpl gasService =new  GasQueryServiceImpl();
////		String s = gasService.queryGas(gasPara);
////		System.out.println(s);
	}

	
}
