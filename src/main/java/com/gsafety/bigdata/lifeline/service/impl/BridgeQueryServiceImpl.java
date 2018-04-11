package com.gsafety.bigdata.lifeline.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.gsafety.bigdata.lifeline.dao.HBaseDao;
import com.gsafety.bigdata.lifeline.dao.PhoenixJdbcDao;
import com.gsafety.bigdata.lifeline.pojo.BridgeParam;
import com.gsafety.bigdata.lifeline.pojo.ResultMessage;
import com.gsafety.bigdata.lifeline.service.BridgeQueryService;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;

@SuppressWarnings("all")
public class BridgeQueryServiceImpl implements BridgeQueryService {

    private static final String[] BRIDGE_ACCE_APP_DAYS={"402884405026354d6003"};
    private static final String[] BRIDGE_DISPMT_APP_DAYS={"402884405026354d6007"};
    private static final String[] BRIDGE_LOWSUMARY_APP_DAYS={"402884405026354d6000","402884405026354d6002","402884405026354d6005","402884405026354d6010","402884405026354d6011"};
    private static final String[] BRIDGE_STRAIN_APP_DAYS={"402884405026354d6004"};
    private static final String[] BRIDGE_DYNDEF_APP_DAYS={"402884405026354d6012"};

    private boolean contain(String[] s,String monitorid){
        return Arrays.asList(s).contains(monitorid);
    }
    private String getTableNameFromMonitorId(String monitorid){
        if(contain(BRIDGE_ACCE_APP_DAYS,monitorid))
            return "BRIDGE_ACCE_APP_DAYS";
        if(contain(BRIDGE_DISPMT_APP_DAYS,monitorid))
            return "BRIDGE_DISPMT_APP_DAYS";
        if(contain(BRIDGE_LOWSUMARY_APP_DAYS,monitorid))
            return "BRIDGE_LOWSUMARY_APP_DAYS";
        if(contain(BRIDGE_STRAIN_APP_DAYS,monitorid))
            return "BRIDGE_STRAIN_APP_DAYS";
        if(contain(BRIDGE_DYNDEF_APP_DAYS,monitorid))
            return "BRIDGE_DYNDEF_APP_DAYS";
        return"";
    }

    private Logger logger = Logger.getLogger(BridgeQueryServiceImpl.class);

    private HBaseDao hBaseDao;
    private JdbcTemplate kylinJdbcTemplate;
    private PhoenixJdbcDao phoenixJdbcDao;
//    private HiveJdbcDao hiveJdbcDao;


    public String queryBridge(BridgeParam bridgeParam) {
        long t1 = System.currentTimeMillis();
        List<Map<String, Object>> lists;
        String json = null;
        try {
            lists = phoenixJdbcDao.queryBridge(bridgeParam.getLocations(), bridgeParam.getTerninal(),bridgeParam.getSensor(), bridgeParam.getStarTime(), bridgeParam.getEndTime(),bridgeParam.getDataType());
            long t2 = System.currentTimeMillis();
            logger.info(" transitionJsonTime:" + (t2 - t1));
            json = JSONObject.toJSONString(lists);
            t2 = System.currentTimeMillis();
            logger.info(" countTime :" + (t2 - t1));
        } catch (Exception e) {
            logger.error(e);
        }
        return json;
    }

    public String queryBridgeFeature(BridgeParam bridgeParam) throws Exception {
        long t1 = System.currentTimeMillis();
        List<Map<String, Object>> lists;
        String json = null;
        try {
            lists = phoenixJdbcDao.queryBridgeFeature(bridgeParam.getLocations(), bridgeParam.getTerninal(),bridgeParam.getSensor(), bridgeParam.getStarTime(), bridgeParam.getEndTime(), bridgeParam.getDataType());
            long t2 = System.currentTimeMillis();
            logger.info(" transitionJsonTime:" + (t2 - t1));
            json = JSONObject.toJSONString(lists);
            t2 = System.currentTimeMillis();
            logger.info(" countTime :" + (t2 - t1));
        } catch (Exception e) {
            logger.error(e);

        }
        return json;
    }

    public String queryBridgeEnviroment(String[] equipids, String monitorid, String stime, String etime) {
        StringBuffer sb = new StringBuffer();
        String table=getTableNameFromMonitorId(monitorid);
        System.out.println(table);
        sb.append("select equip_id as ids,equipmentname as name ,monitor_id as mid,dt as times, max(dvalues) as maxs, min(dvalues) as mins, avg(dvalues) as avgs from "+table);
        sb.append(" where 1=1");
        if (equipids != null && equipids.length > 0) {
            sb.append(" and equip_id in (").append(arrayToString(equipids)).append(") ");
        }
        if (!StringUtils.isEmpty(monitorid)) {
            sb.append(" and monitor_id = '").append(monitorid).append("'");
        }
        if (!StringUtils.isEmpty(stime) && !StringUtils.isEmpty(etime)) {
            stime = stime.replace("-", "");
            etime = etime.replace("-", "");
            sb.append(" and dt between '").append(stime).append("' and '").append(etime).append("' ");
        }
        sb.append(" group by(equip_id,dt,equipmentname,monitor_id) order by dt asc");
//        JdbcTemplate kylinJdbcTemplate = (JdbcTemplate) SpringUtil.getBean("kylinjdbcTemplate");
        if (kylinJdbcTemplate == null) {
            System.out.println("null");
            logger.info("kylinJdbcTemplate为空");
//			return toJson ResultMessage.getMessageFromCode()()("101","数据库连接对象为空"));
            return "";
        }
        logger.info(sb);
        System.out.println(sb.toString());
        List<?> values;
        try {
            values = kylinJdbcTemplate.queryForList(sb.toString());
        } catch (Exception e) {
            logger.info(e);
//			return toJson ResultMessage.getMessageFromCode()()("201","查询过程异常"));
            return "";
        }
        if (values == null || values.size() < 1) {
            logger.info("没有查询结果values:" + values);
//			return toJson ResultMessage.getMessageFromCode()()("301","查询结果为空"));
            return "";
        }
        JSONObject info = JSONObject.parseObject("{}");
        JSONObject item;
        String[] fields = {"maxs", "mins", "avgs", "mid"};
        ArrayList data;
        for (int i = 0; i < values.size(); i++) {
            Map<String, String> map = (Map<String, String>) values.get(i);
            JSONObject value = JSONObject.parseObject("{}");
            if (!StringUtils.isEmpty(map.get("times"))) {
                StringBuffer sbtimes = new StringBuffer(map.get("times"));
                sbtimes.insert(6, "-");
                sbtimes.insert(4, "-");
                value.put("times", sbtimes.toString());
            } else {
                value.put("times", "");
            }
            for (int j = 0; j < fields.length; j++) {
                value.put(fields[j], map.get(fields[j]));
            }
            if (info.containsKey(map.get("ids"))) {
                item = info.getJSONObject(map.get("ids"));
                data = (ArrayList) item.get("data");
                data.add(value);
            } else {
                item = JSONObject.parseObject("{}");
                item.put("name", map.get("name"));
                data = new ArrayList();
                data.add(value);
                item.put("data", data);
            }
            info.put(map.get("ids"), item);
        }
        return info.toJSONString();
    }

    @Override
    public ResultMessage queryBridgeV2(BridgeParam bridgeParam) {
        long t1 = System.currentTimeMillis();
        if (phoenixJdbcDao == null) {
            return ResultMessage.getMessageFromCode("102");
        }
        List<Map<String, Object>> lists;
        try {
            lists = phoenixJdbcDao.queryBridge(bridgeParam.getLocations(), bridgeParam.getTerninal(),
                    bridgeParam.getSensor(), bridgeParam.getStarTime(), bridgeParam.getEndTime(),bridgeParam.getDataType());
        } catch (Exception e) {
            logger.info(e);
            return ResultMessage.getMessageFromCode("201");
        }

        if (lists == null || lists.size() < 1) {
            return ResultMessage.getMessageFromCode("301");
        }
        long t2 = System.currentTimeMillis();
        logger.info(" transitionJsonTime:" + (t2 - t1));
        String json = toJson(lists);
        if ("401".equals(json)) {
            return ResultMessage.getMessageFromCode("401");
        }
        t2 = System.currentTimeMillis();
        logger.info(" countTime :" + (t2 - t1));
        return new ResultMessage("000", json);
    }

    @Override
    public ResultMessage queryBridgeFeatureV2(BridgeParam bridgeParam) {
        long t1 = System.currentTimeMillis();
        if (phoenixJdbcDao == null) {
            return ResultMessage.getMessageFromCode("102");
        }
        List<Map<String, Object>> lists;
        try {
            lists = phoenixJdbcDao.queryBridgeFeature(bridgeParam.getLocations(), bridgeParam.getTerninal(),
                    bridgeParam.getSensor(), bridgeParam.getStarTime(), bridgeParam.getEndTime(), bridgeParam.getDataType());
        } catch (Exception e) {
            logger.info(e);
            return ResultMessage.getMessageFromCode("201");
        }

        if (lists == null || lists.size() < 1) {
            return ResultMessage.getMessageFromCode("301");
        }
        long t2 = System.currentTimeMillis();

        logger.info(" transitionJsonTime:" + (t2 - t1));
        String json = toJson(lists);
        if ("401".equals(json)) {
            return ResultMessage.getMessageFromCode("401");
        }
        t2 = System.currentTimeMillis();
        logger.info(" countTime :" + (t2 - t1));

        return new ResultMessage("000", json);
    }

    @Override
    public ResultMessage queryBridgeEnviromentV2(String[] equipids, String monitorid, String stime, String etime) {
        StringBuffer sb = new StringBuffer();
        System.out.println(getTableNameFromMonitorId(monitorid));
        sb.append("select equip_id as ids,equipmentname as name ,monitor_id as mid,dt as times, max(dvalues) as maxs, min(dvalues) as mins, avg(dvalues) as avgs from "+getTableNameFromMonitorId(monitorid));
        sb.append(" where 1=1");
        if (equipids != null && equipids.length > 0) {
            sb.append(" and equip_id in (").append(arrayToString(equipids)).append(") ");
        }
        if (!StringUtils.isEmpty(monitorid)) {
            sb.append(" and monitor_id = '").append(monitorid).append("'");
        }
        if (!StringUtils.isEmpty(stime) && !StringUtils.isEmpty(etime)) {
            stime = stime.replace("-", "");
            etime = etime.replace("-", "");
            sb.append(" and dt between '").append(stime).append("' and '").append(etime).append("' ");
        }
        sb.append(" group by(equip_id,dt,equipmentname,monitor_id) order by dt asc");
//        JdbcTemplate kylinJdbcTemplate = (JdbcTemplate) SpringUtil.getBean("kylinjdbcTemplate");
        if (kylinJdbcTemplate == null) {
            System.out.println("null");
            logger.info("kylinJdbcTemplate为空");
            return ResultMessage.getMessageFromCode("101");
        }
        System.out.println(sb);
        logger.info(sb);
        List<?> values;
        try {
            values = kylinJdbcTemplate.queryForList(sb.toString());
        } catch (Exception e) {
            logger.info(e);
            return ResultMessage.getMessageFromCode("201");
        }
        if (values == null || values.size() < 1) {
            logger.info("没有查询结果values:" + values);
            return ResultMessage.getMessageFromCode("301");
        }
        JSONObject info = JSONObject.parseObject("{}");
        JSONObject item;
        String[] fields = {"maxs", "mins", "avgs", "mid"};
        ArrayList data;
        for (int i = 0; i < values.size(); i++) {
            Map<String, String> map = (Map<String, String>) values.get(i);
            JSONObject value = JSONObject.parseObject("{}");
            if (!StringUtils.isEmpty(map.get("times"))) {
                StringBuffer sbtimes = new StringBuffer(map.get("times"));
                sbtimes.insert(6, "-");
                sbtimes.insert(4, "-");
                value.put("times", sbtimes.toString());
            } else {
                value.put("times", "");
            }
            for (int j = 0; j < fields.length; j++) {
                value.put(fields[j], map.get(fields[j]));
            }
            if (info.containsKey(map.get("ids"))) {
                item = info.getJSONObject(map.get("ids"));
                data = (ArrayList) item.get("data");
                data.add(value);
            } else {
                item = JSONObject.parseObject("{}");
                item.put("name", map.get("name"));
                data = new ArrayList();
                data.add(value);
                item.put("data", data);
            }
            info.put(map.get("ids"), item);
        }
        return new ResultMessage("000", info.toJSONString());
    }

    public ResultMessage queryBridgeEnviromentJdbcV2(String[] equipids, String monitorid, String stime, String etime) {
        StringBuffer sb = new StringBuffer();
        sb.append("select equip_id as ids,equipmentname as name ,monitor_id as mid,dt as times, max(dvalues) as maxs, min(dvalues) as mins, avg(dvalues) as avgs from "+getTableNameFromMonitorId(monitorid));
        sb.append(" where 1=1");
        if (equipids != null && equipids.length > 0) {
            sb.append(" and equip_id in (").append(arrayToString(equipids)).append(") ");
        }
        if (!StringUtils.isEmpty(monitorid)) {
            sb.append(" and monitor_id = '").append(monitorid).append("'");
        }
        if (!StringUtils.isEmpty(stime) && !StringUtils.isEmpty(etime)) {
            stime = stime.replace("-", "");
            etime = etime.replace("-", "");
            sb.append(" and dt between '").append(stime).append("' and '").append(etime).append("' ");
        }
        sb.append(" group by(equip_id,dt,equipmentname,monitor_id) order by dt asc");
//        JdbcTemplate kylinJdbcTemplate = (JdbcTemplate) SpringUtil.getBean("kylinjdbcTemplate");
        if (kylinJdbcTemplate == null) {
            System.out.println("null");
            logger.info("kylinJdbcTemplate为空");
            return ResultMessage.getMessageFromCode("101");
        }
        logger.info(sb);
        List<?> values;
        try {
            values = kylinJdbcTemplate.queryForList(sb.toString());
        } catch (Exception e) {
            logger.info(e);
            return ResultMessage.getMessageFromCode("201");
        }
        if (values == null || values.size() < 1) {
            logger.info("没有查询结果values:" + values);
            return ResultMessage.getMessageFromCode("301");
        }
        JSONObject info = JSONObject.parseObject("{}");
        JSONObject item;
        String[] fields = {"maxs", "mins", "avgs", "mid"};
        ArrayList data;
        for (int i = 0; i < values.size(); i++) {
            Map<String, String> map = (Map<String, String>) values.get(i);
            JSONObject value = JSONObject.parseObject("{}");
            if (!StringUtils.isEmpty(map.get("times"))) {
                StringBuffer sbtimes = new StringBuffer(map.get("times"));
                sbtimes.insert(6, "-");
                sbtimes.insert(4, "-");
                value.put("times", sbtimes.toString());
            } else {
                value.put("times", "");
            }
            for (int j = 0; j < fields.length; j++) {
                value.put(fields[j], map.get(fields[j]));
            }
            if (info.containsKey(map.get("ids"))) {
                item = info.getJSONObject(map.get("ids"));
                data = (ArrayList) item.get("data");
                data.add(value);
            } else {
                item = JSONObject.parseObject("{}");
                item.put("name", map.get("name"));
                data = new ArrayList();
                data.add(value);
                item.put("data", data);
            }
            info.put(map.get("ids"), item);
        }
        return new ResultMessage("000", info.toJSONString());
    }

    public void queryKylinTest(String sql) {
//		StringBuffer sb = new StringBuffer();
//		sb.append("select * from APP.BRIDGE_ACCE_APP_DAYS limit 10 ");
//		sb.append(" where 1=1");
//		JdbcTemplate kylinJdbcTemplate = (JdbcTemplate) SpringUtil.getBean("kylinjdbcTemplate");
        if (StringUtils.isEmpty(sql)) {
            System.out.println("sql语句不能为空");
        }
        if (kylinJdbcTemplate == null) {
            System.out.println("kylinJdbcTemplate为空");
            logger.info("kylinJdbcTemplate为空");
        }
        logger.info(sql);
        List<?> values = new ArrayList<>();
        try {
            values = kylinJdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.info(e);
            e.printStackTrace();
        }
        if (values == null || values.size() < 1) {
            logger.info("没有查询结果values:" + values);
        }
        System.out.println(values);
    }

    public List<List<Double>> queryBridgeModel(String locations, String terninal, String sensor, String starTime, String endTime, String table) {
        System.out.println("query bridge model>>>>>>>>>>");
        return hBaseDao.queryV2(locations,terninal,sensor,starTime,endTime,table);
    }

    public List<Map<String, Object>> phoenixJdbcTest() throws Exception {
        List<Map<String, Object>> mapList = phoenixJdbcDao.queryBridgeFeature("00000000", "HF_JZDL_00000003", "4_4", "2017-01-06 16:00:00", "2017-09-06 16:59:00", "PV");
        return null;
    }

    public List<double[][]> queryBridgeDouble(List<BridgeParam> list) {
//        ArrayList<double[][]> doubles = new ArrayList<>();
//        for (BridgeParam bp:list){
//            try {
//                double[][] doubles1 = phoenixJdbcDao.queryBridgeDouble(bp.getLocations(), bp.getTerninal(), bp.getSensor(), bp.getStarTime(), bp.getEndTime(), bp.getDataType());
//                doubles.add(doubles1);
//            } catch (Exception e) {
//                e.printStackTrace();
//                logger.info(e);
//            }
//        }
//        return doubles;
        return null;
    }
    public String queryDynamic(BridgeParam bridgeParam) {
        long t1 = System.currentTimeMillis();
        List<Map<String, Object>> lists;
        String json = null;
        try {
            lists = phoenixJdbcDao.queryDynamic(bridgeParam.getLocations(), bridgeParam.getTerninal(),bridgeParam.getSensor(), bridgeParam.getStarTime(), bridgeParam.getEndTime());
            long t2 = System.currentTimeMillis();
            logger.info(" transitionJsonTime:" + (t2 - t1));
            json = JSONObject.toJSONString(lists);
            t2 = System.currentTimeMillis();
            logger.info(" countTime :" + (t2 - t1));
        } catch (Exception e) {
            logger.error(e);
        }
        return json;
    }
    private static String arrayToString(String[] array) {
        StringBuffer vehiclepoint = new StringBuffer();
        if (array != null) {
            for (int i = 0; i < array.length; i++) {
                vehiclepoint.append("'").append(array[i]).append("'");
                if (i < array.length - 1) {
                    vehiclepoint.append(", ");
                }
            }
        } else {
            vehiclepoint.append("''");
        }
        return vehiclepoint.toString();
    }

    private String toJson(Object o) {
        try {
            String result = JSONObject.toJSONString(o);
            return result;
        } catch (Exception e) {
            logger.info(e);
            return "401";
        }
    }

    public static void main(String[] args) {
        //测试桥梁接口

        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("phoenix-jdbc.xml");
        PhoenixJdbcDao phoenixJdbcDao = context.getBean("phoenixJdbcDao", PhoenixJdbcDao.class);
//        BridgeQueryServiceImpl bridgeQueryService = context.getBean("BridgeQueryService", BridgeQueryServiceImpl.class);
        BridgeQueryServiceImpl bridgeQueryService = new BridgeQueryServiceImpl();
        bridgeQueryService.setPhoenixJdbcDao(phoenixJdbcDao);
        bridgeQueryService.sethBaseDao(new HBaseDao());
        List<List<Double>> model = bridgeQueryService.queryBridgeModel("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-11-05 16:14:47", "2017-11-06 16:14:47", "");
        System.out.println(model);
//        BridgeParam bridgeParam = new BridgeParam("00000000", "HF_JZDL_00000003", "5_12", "2017-10-19 16:58:00", "2017-10-19 16:59:00","S10M");
//        BridgeParam bridgeParam = new BridgeParam("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-10-25 16:00:00", "2017-10-25 16:59:00", "");
//        ArrayList<BridgeParam> objects = new ArrayList<>();
//        objects.add(bridgeParam);
//        List<double[][]> doubles = bridgeQueryService.queryBridgeDouble(objects);
//        System.out.println(doubles);
//		 System.out.println(bridgeQueryService.queryBridgeFeatureV2(bridgeParam));
//		 System.out.println(bridgeQueryService.queryBridgeV2(bridgeParam));
//		 System.out.println(bridgeQueryService.queryBridgeEnviromentV2(new String[]{"011PHDQDHAz1"},"402884405026354d6003","2017-10-01","2017-10-25"));
//		 System.out.println(bridgeQueryService.queryBridgeDouble();

    }

    public void sethBaseDao(HBaseDao hBaseDao) {
        this.hBaseDao = hBaseDao;
    }

    public void setKylinJdbcTemplate(JdbcTemplate kylinJdbcTemplate) {
        this.kylinJdbcTemplate = kylinJdbcTemplate;
    }

    public void setPhoenixJdbcDao(PhoenixJdbcDao phoenixJdbcDao) {
        this.phoenixJdbcDao = phoenixJdbcDao;
    }
//    public void setHiveJdbcDao(HiveJdbcDao hiveJdbcDao) {
//        this.hiveJdbcDao = hiveJdbcDao;
//    }
}
