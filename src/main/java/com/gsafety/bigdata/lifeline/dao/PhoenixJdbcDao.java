package com.gsafety.bigdata.lifeline.dao;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by zhangdm on 2017/9/7.
 */
@SuppressWarnings("all")
public class PhoenixJdbcDao {

    Logger logger = Logger.getLogger(PhoenixJdbcDao.class);
    private JdbcTemplate phoenixJdbcTemplate;

    private int PERCENT;
    private static final String COLUMNS = "\"PK\",\"dataType\",\"level\",\"time\",\"values\"";
//    private static final String COLUMNS = "\"dataType\",\"level\",\"rtime\" as \"time\",round(to_number(\"values\"),2) as \"values\"";

    public List<Map<String, Object>> queryBridge(String locations, String terninal, String sensor, String starTime, String endTime, String dataType) throws Exception {
        String table = "";
        boolean full = false;
        if ("TENDENCY_10M".equals(dataType)) {
            table = "BRIDGE_TENDENCY_10M";
            dataType = "";
        } else if (fullData(starTime, endTime) == 1) {
            table = "bridge_" + terninal;
            full = true;
        } else if (fullData(starTime, endTime) == 2) {
            table = "bridge_" + terninal + "_1S";
            full = true;
        } else {
            table = "BRIDGE_FEATURE_10M";
        }
        System.out.println(table);
        return queryV2(locations, terninal, sensor, starTime, endTime, table, dataType, PERCENT, full);
    }

    public List<Map<String, Object>> queryBridgeTest(String locations, String terninal, String sensor, String starTime, String endTime, String dataType) throws Exception{
//        String table="bridge_" + terninal + "_1S";
        String table="bridge_" + terninal;
//        String table="BRIDGE_FEATURE_10M";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, dataType, PERCENT, true);
    }

    public List<Map<String, Object>> queryBridgeDouble(String locations, String terninal, String sensor, String starTime, String endTime, String dataType) throws Exception {
        String table = "bridge_" + terninal;
        System.out.println(table);
        return query(locations, terninal, sensor, starTime, endTime, table, dataType);
    }

    public double[][] queryBridgeDoubleV2(String locations, String terninal, String sensor, String starTime, String endTime, String dataType) throws Exception {
        String table = "bridge_" + terninal;
        System.out.println(table);
        return queryNew(locations, terninal, sensor, starTime, endTime, table, dataType);
    }

    public List<Map<String, Object>> queryGas(String locations, String terninal, String sensor, String starTime, String endTime) throws Exception {
        String table = "gas_1s";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, null, PERCENT, false);
    }

    public List<Map<String, Object>> queryBridgeFeature(String locations, String terninal, String sensor, String starTime, String endTime, String dataType) throws Exception {
        String table = "BRIDGE_FEATURE_10M";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, dataType, PERCENT, false);
    }

    public List<Map<String, Object>> queryWater(String locations, String terninal, String sensor, String starTime, String endTime, String sample) throws Exception {
        String table = "water_1s";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, null, PERCENT, false);
    }

    public List<Map<String, Object>> queryWaterFlow(String locations, String terninal, String sensor, String starTime, String endTime) throws Exception {
        String table = "WATER_FLOW";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, null, PERCENT, false);
    }

    public List<Map<String, Object>> queryDrain(String locations, String terninal, String sensor, String starTime, String endTime) throws Exception {
        String table = "DRAIN_1S";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, null, PERCENT, false);
    }

    public List<Map<String, Object>> queryHeat(String locations, String terninal, String sensor, String starTime, String endTime) throws Exception {
        String table = "HEAT_1S";
        return queryV2(locations, terninal, sensor, starTime, endTime, table, null, PERCENT, false);
    }

    public List<Map<String, Object>> queryBridgeDynamic(String locations, String terninal, String sensor, String starTime, String endTime) throws Exception {
        return queryDynamic(locations, terninal, sensor, starTime, endTime);
    }



    //
    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final long t1 = 10 * 60 * 1000l;
    private static final long t2 = 24 * 60 * 60 * 1000l;

    private int fullData(String startTime, String endTime) throws Exception {
        Date start = format.parse(startTime);
        Date end = format.parse(endTime);
        long dif = end.getTime() - start.getTime();
        if (dif <= t1) {
            return 1;
        } else if (dif <= t2) {
            return 2;
        } else {
            return 3;
        }
    }

    private double[][] queryNew(String locations, String terninal, String sensor, String starTime, String endTime, String table, String dataType) throws Exception {
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        String md5String = sensor + terninal;
        String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = dateFormat.parse(starTime).getTime();
        long end = dateFormat.parse(endTime).getTime();

        String startrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - end);
        String endrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - start);

        logger.info("Startrow :" + startrowkey);
        logger.info("endrow :" + endrowkey);
        System.out.println(startrowkey);
        System.out.println(endrowkey);
        String baseSql = "select \"PK\",\"values\",\"time\" from " + table.toUpperCase() + " where \"PK\" between \'" + startrowkey + "\' and \'" + endrowkey + "\'";
        StringBuffer stringBuffer = new StringBuffer(baseSql);

//        if(!StringUtils.isEmpty(locations)) {
//            stringBuffer.append(" and \"location\" = " + "\'" + locations + "\'");
//        }
        if (!StringUtils.isEmpty(dataType)) {
            stringBuffer.append(" and \"dataType\" = " + "\'" + dataType + "\'");
        }
//        stringBuffer.append(" order by \'PK\'");

        System.out.println(locations);
        System.out.println(stringBuffer.toString());
        //小于24小时的数据全量返回，大于24小时数据返回最多1万条
        long t1 = System.currentTimeMillis();
        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(stringBuffer.toString());
        long t2 = System.currentTimeMillis();
        System.out.println("time>>>>>>>>" + (t2 - t1));
        System.out.println(maps.size() + "............................................");
        logger.info(maps.size());
        if (maps == null || maps.size() == 0)
            return new double[0][0];
        logger.info(stringBuffer.toString());
        long t3 = System.currentTimeMillis();
        Collections.sort(maps, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> stringObjectMap, Map<String, Object> t1) {
                return t1.get("time").toString().compareTo(stringObjectMap.get("time").toString());
            }
        });
        long t4 = System.currentTimeMillis();
        System.out.println("t4-t3>>>>>>>>>>>>>>>" + (t4 - t3));
        double[][] doubles = new double[maps.size()][1];
        for (int j = 0; j < maps.size(); j++) {
            if (maps.get(j).get("values") != null) {
                String[] values = ((String) maps.get(j).get("values")).split(",");
                DecimalFormat df = new DecimalFormat("#.00");
                double[] doubles1 = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    Double doublePar = Double.parseDouble(values[i]);
                    String doubleValue = df.format(doublePar);
                    doubles1[i] = Double.parseDouble(doubleValue);
                }
                doubles[j] = doubles1;
            }
        }
        return doubles;
    }


    private List<Map<String, Object>> query(String locations, String terninal, String sensor, String starTime, String endTime, String table, String dataType) throws Exception {
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        String md5String = sensor + terninal;
        String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = dateFormat.parse(starTime).getTime();
        long end = dateFormat.parse(endTime).getTime();

        String startrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - end);
        String endrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - start);

        logger.info("Startrow :" + startrowkey);
        logger.info("endrow :" + endrowkey);
        System.out.println(startrowkey);
        System.out.println(endrowkey);
        String baseSql = "select \"values\",\"time\" from " + table.toUpperCase() + " where \"PK\" between \'" + startrowkey + "\' and \'" + endrowkey + "\'";
        StringBuffer stringBuffer = new StringBuffer(baseSql);

//        if(!StringUtils.isEmpty(locations)) {
//            stringBuffer.append(" and \"location\" = " + "\'" + locations + "\'");
//        }
        if (!StringUtils.isEmpty(dataType)) {
            stringBuffer.append(" and \"dataType\" = " + "\'" + dataType + "\'");
        }
        stringBuffer.append(" order by \'time\'");

        System.out.println(locations);
        System.out.println(stringBuffer.toString());
        //小于24小时的数据全量返回，大于24小时数据返回最多1万条
        long t1 = System.currentTimeMillis();
        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(stringBuffer.toString());
//        long t2=System.currentTimeMillis();
//        System.out.println("time>>>>>>>>"+(t2-t1));
//        System.out.println(maps.size() + "............................................");
//        logger.info(maps.size());
//        if (maps == null || maps.size() == 0)
//            return new double[0][0];
//        List<Map<String, Object>> mapsReturn = new ArrayList<>();
//        if (maps.size()>10000) {
//            System.out.println("in.....................");
////            stringBuffer.append(" limit 10000");
//            Iterator<Map<String, Object>> iterator = maps.iterator();
//            int rowCount = maps.size();
//            int returnCount = 5000;
//            int count = 0;
//            while (iterator.hasNext()) {
//                count++;
//                if ((count % (rowCount / returnCount)) == 1) {
//                    mapsReturn.add(iterator.next());
//                } else {
//                    iterator.next();
//                }
//            }
//        } else {
//            mapsReturn = maps;
//        }
//        System.out.println(mapsReturn.size() + ".............................................");
//        logger.info(stringBuffer.toString());
//        Collections.sort(mapsReturn, new Comparator<Map<String, Object>>() {
//            @Override
//            public int compare(Map<String, Object> stringObjectMap, Map<String, Object> t1) {
//                return t1.get("time").toString().compareTo(stringObjectMap.get("time").toString());
//            }
//        });
//        double[][] doubles = new double[mapsReturn.size()][1];
//        for (int j=0;j<mapsReturn.size();j++) {
//            if (mapsReturn.get(j).get("values") != null) {
//                String[] values = ((String) mapsReturn.get(j).get("values")).split(",");
//                DecimalFormat df = new DecimalFormat("#.00");
//                double[] doubles1 = new double[values.length];
//                for(int i=0;i<values.length;i++){
//                    Double doublePar = Double.parseDouble(values[i]);
//                    String doubleValue = df.format(doublePar);
//                    doubles1[i]=Double.parseDouble(doubleValue);
//                }
//                doubles[j]=doubles1;
//            }
//        }
//        return doubles;
        return maps;
    }


    /**
     * @param locations
     * @param terminal
     * @param sensor
     * @param starTime
     * @param endTime
     * @return
     * @describe 查询动态阀值
     */
    public List<Map<String, Object>> queryDynamic(String locations, String terminal, String sensor, String starTime, String endTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuffer sl = new StringBuffer("select *  from BRIDGE_DYNAMIC_10M where 1=1");
        if (StringUtils.isNotEmpty(locations)) {
            sl.append(" and \"location\" ='").append(locations).append("'");
        }
        if (StringUtils.isNotEmpty(terminal)) {
            sl.append(" and \"terminal\" ='").append(terminal).append("'");
        }
        if (StringUtils.isNotEmpty(sensor)) {
            sl.append(" and \"sensor\" ='").append(sensor).append("'");
        }
        Date date = new Date();
        if (StringUtils.isNotEmpty(starTime)) {
            sl.append(" and \"time\" >='").append(starTime).append("'");
            System.out.println(Long.valueOf(Timestamp.valueOf(starTime).getTime()));
        }
        if (StringUtils.isNotEmpty(endTime)) {
            sl.append(" and \"time\" <='").append(endTime).append("'");
            System.out.println(Long.valueOf(Timestamp.valueOf(endTime).getTime()));
        }
        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(sl.toString());
        return maps;
    }
//
//    public List<Map<String, Object>> queryHeat(String locations, String terminal, String sensor, String starTime, String endTime) {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        StringBuffer sl = new StringBuffer("select *  from GAS_1S where 1=1");
//        if (StringUtils.isNotEmpty(locations)) {
//            sl.append(" and \"location\" ='").append(locations).append("'");
//        }
//        if (StringUtils.isNotEmpty(terminal)) {
//            sl.append(" and \"terminal\" ='").append(terminal).append("'");
//        }
//        if (StringUtils.isNotEmpty(sensor)) {
//            sl.append(" and \"sensor\" ='").append(sensor).append("'");
//        }
//        Date date = new Date();
//        if (StringUtils.isNotEmpty(starTime)) {
//            sl.append(" and \"time\" >='").append(starTime).append("'");
//            System.out.println(Long.valueOf(Timestamp.valueOf(starTime).getTime()));
//        }
//        if (StringUtils.isNotEmpty(endTime)) {
//            sl.append(" and \"time\" <='").append(endTime).append("'");
//            System.out.println(Long.valueOf(Timestamp.valueOf(endTime).getTime()));
//        }
//        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(sl.toString());
//        return maps;
//    }

    /**
     * @param locations
     * @param terminal
     * @param sensor
     * @param starTime
     * @param endTime
     * @return
     * @Describe 查询热力专项的上下游值
     */
//    public List<Map<String, Object>> queryHeat(String locations, String terminal, String sensor, String starTime, String endTime) {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        StringBuffer sl = new StringBuffer("select *  from GAS_1S where 1=1");
//        if (StringUtils.isNotEmpty(locations)) {
//            sl.append(" and \"location\" ='").append(locations).append("'");
//        }
//        if (StringUtils.isNotEmpty(terminal)) {
//            sl.append(" and \"terminal\" ='").append(terminal).append("'");
//        }
//        if (StringUtils.isNotEmpty(sensor)) {
//            sl.append(" and \"sensor\" ='").append(sensor).append("'");
//        }
//        Date date = new Date();
//        if (StringUtils.isNotEmpty(starTime)) {
//            sl.append(" and \"time\" >='").append(starTime).append("'");
//            System.out.println(Long.valueOf(Timestamp.valueOf(starTime).getTime()));
//        }
//        if (StringUtils.isNotEmpty(endTime)) {
//            sl.append(" and \"time\" <='").append(endTime).append("'");
//            System.out.println(Long.valueOf(Timestamp.valueOf(endTime).getTime()));
//        }
//        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(sl.toString());
//        return maps;
//    }

    private List<Map<String, Object>> queryV2(String locations, String terninal, String sensor, String starTime, String endTime, String table, String dataType, int percent, boolean full) throws Exception {
        long tStart = System.currentTimeMillis();
        SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        String md5String = sensor + terninal;
        String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = dateFormat.parse(starTime).getTime();
        long end = dateFormat.parse(endTime).getTime();

        String startrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - end);
        String endrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
                + (Long.MAX_VALUE - start);

        logger.info("Startrow :" + startrowkey);
        logger.info("endrow :" + endrowkey);
        System.out.println(startrowkey);
        System.out.println(endrowkey);
        String baseSql = "select " + COLUMNS + " from " + table.toUpperCase() + " where \"PK\" between \'" + startrowkey + "\' and \'" + endrowkey + "\'";
//        String baseSql = "select " + COLUMNS + " from " + table.toUpperCase() + " where \"ROW\" between \'" + startrowkey + "\' and \'" + endrowkey + "\'";
        StringBuffer stringBuffer = new StringBuffer(baseSql);

//        if(!StringUtils.isEmpty(locations)) {
//            stringBuffer.append(" and \"location\" = " + "\'" + locations + "\'");
//        }
        if (!StringUtils.isEmpty(dataType)) {
            stringBuffer.append(" and \"dataType\" = " + "\'" + dataType + "\'");
        }
        if (percent > 1) {
            stringBuffer.append(" and \"seconds\" in " + getPercent(percent));
        }
        System.out.println(stringBuffer.toString());
        //小于24小时的数据全量返回，大于24小时数据返回最多1万条
        List<Map<String, Object>> maps = phoenixJdbcTemplate.queryForList(stringBuffer.toString());
        System.out.println(maps.size() + "............................................");
        logger.info(maps.size());
        if (maps == null || maps.size() == 0) {
            return null;
        }
        if (full) {
            System.out.println("full......................back");
            long t1 = System.currentTimeMillis();
            sortAsTime(maps);
            long t2 = System.currentTimeMillis();
            System.out.println("排序时间.............." + (t2 - t1));
            valueFormat(dateFormat2, maps);
            long tEnd = System.currentTimeMillis();
            System.out.println("查询时间》》》》》》》》》》》"+(tEnd-tStart));
            return maps;
        }
        List<Map<String, Object>> mapsReturn = new ArrayList<>();
        if (maps.size() > 10000) {
            System.out.println("in.....................");
//            stringBuffer.append(" limit 10000");
            Iterator<Map<String, Object>> iterator = maps.iterator();
            int rowCount = maps.size();
            int returnCount = 5000;
            int count = 0;
            while (iterator.hasNext()) {
                count++;
                if ((count % (rowCount / returnCount)) == 1) {
                    mapsReturn.add(iterator.next());
                } else {
                    iterator.next();
                }
            }
        } else {
            mapsReturn = maps;
        }
        System.out.println(mapsReturn.size() + ".............................................");
        logger.info(stringBuffer.toString());
        valueFormat(dateFormat2, mapsReturn);
        sortAsTime(mapsReturn);
        long tEnd = System.currentTimeMillis();
        System.out.println("查询时间》》》》》》》》》》》"+(tEnd-tStart));
        return mapsReturn;
    }

    private void sortAsTime(List<Map<String, Object>> mapsReturn) {
        Collections.sort(mapsReturn, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> stringObjectMap, Map<String, Object> t1) {
                return t1.get("time").toString().compareTo(stringObjectMap.get("time").toString());
            }
        });
    }

    private void valueFormat(SimpleDateFormat dateFormat2, List<Map<String, Object>> mapsReturn) throws ParseException {
        for (Map map : mapsReturn) {
            if (map.get("values") != null) {
                String[] values = ((String) map.get("values")).split(",");
                List<Double> doubleArray = new ArrayList<Double>();
                DecimalFormat df = new DecimalFormat("#.00");
                for (String value : values) {
                    Double doublePar = Double.parseDouble(value);
                    String doubleValue = df.format(doublePar);
                    doubleArray.add(new Double(doubleValue));
                }
                map.put("values", doubleArray);
            }
            if (map.get("time") != null) {
                long time2 = dateFormat2.parse(map.get("time").toString()).getTime();
                map.put("time", time2);
            }
        }
    }

    private static String getPercent(int n) {
        ArrayList<String> in = new ArrayList<>();
        ArrayList<String> out = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            for (int j = 0; j < 10; j++) {
                in.add("\"" + i + "" + j + "\"");
            }
        }
        for (int i = 0; i < 60; i++) {
            if (i % n == 0) {
                out.add(in.get(i));
            }
        }
        System.out.println(out.toString().replace("[", "(").replace("]", ")"));
        return out.toString().replace("[", "(").replace("]", ")");
    }


    public void setPhoenixJdbcTemplate(JdbcTemplate phoenixJdbcTemplate) {
        this.phoenixJdbcTemplate = phoenixJdbcTemplate;
    }

    public int getPERCENT() {
        return PERCENT;
    }

    public void setPERCENT(int PERCENT) {
        this.PERCENT = PERCENT;
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ApplicationContext ac = new ClassPathXmlApplicationContext("phoenix-jdbc.xml");
        PhoenixJdbcDao phoenixJdbcdao = ac.getBean("phoenixJdbcDao", PhoenixJdbcDao.class);
        List<Map<String, Object>> maps = phoenixJdbcdao.queryGas("340104-06-00-021-008349", "NDIR001711050221", "0_0", "2018-01-01 00:00:00", "2018-03-08 00:00:00");
//        List<Map<String, Object>> maps = phoenixJdbcdao.queryBridgeTest("", "HF_FHDD_DY000001", "39_7", "2018-03-15 22:20:00", "2018-03-16 01:35:00", null);
//        System.out.println(maps);
        System.setOut(new PrintStream(new File("E:\\bur.txt")));
        for(Map map :maps){
            Object avg = ((ArrayList) (map.get("values"))).get(0);
            if((Double)avg>50) {
                System.out.println("\n\n\n\n\n\n\n>>>>>>>>>>>>>>>>>>>");
                System.out.println(map);
                System.out.println(">>>>>>>>>>>>>>>>>>>\n\n\n\n\n\n\n");
            }else{
                System.out.println(map);
            }
        }


//        List<Map<String, Object>> mapList;
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-18 17:00:50", "2018-01-18 17:00:54", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-18 13:30:40", "2018-01-18 13:30:59", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-18 18:20:39", "2018-01-18 18:20:53", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-19 07:00:40", "2018-01-19 07:00:50", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-19 08:40:40", "2018-01-19 08:40:50", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "16_1", "2018-01-19 16:50:40", "2018-01-19 17:20:50", "");
//        List<Map<String, Object>> list = phoenixJdbcdao.queryBridgeTest("00000000", "HF_FHDD_DY000001", "18_9", "2018-02-01 14:07:00", "2018-02-01 14:08:54", "");
//        System.out.println(list);
//        Object result = JSONObject.toJSON(list);
////        JSONArray objects = JSONArray.parseArray((String)result);
//        for(Object o :(JSONArray)result){
//            Object level = ((JSONObject) o).get("level");
//            Object time = ((JSONObject) o).get("time");
////            String timeFormat = format.format(new Date(Long.parseLong(time.toString())));
////            ((JSONObject) o).put("time",timeFormat);
////            if(Integer.parseInt(level.toString())>0){
//                System.out.println(o);
////            }
//        }
//        System.out.println(list);
//        phoenixJdbcdao.queryBridge("00000000", "HF_FHDD_DY000001", "19_5", "2018-01-17 16:55:00", "2018-01-18 16:55:00", "");
//        long t1 = System.currentTimeMillis();
//        phoenixJdbcdao.queryBridge("00000000", "HF_FHDD_DY000001", "19_5", "2018-01-17 16:55:00", "2018-01-18 16:55:00", "");
//        long t2 = System.currentTimeMillis();
//        System.out.println(t2-t1);
////        for (int i=0;i<100;i++){
//        mapList = phoenixJdbcdao.queryBridgeFeature("00000000", "HF_JZDL_00000003", "4_4", "2017-01-06 16:00:00", "2017-10-20 16:59:00", "TENDENCY_10M");
//        List<Map<String, Object>> maps = phoenixJdbcdao.queryGas("00000000", "1711060731", "0_0", "2017-12-01 16:14:47", "2017-12-06 16:14:47");
//        System.out.println(maps);
//        List<Map<String, Object>> maps = phoenixJdbcdao.queryHeat("00000000", "HF_FHDD_DY000001", "39_7", "2017-12-01 16:14:47", "2017-12-07 16:14:47");
//        System.out.println(maps);
//        System.out.println(Arrays.toString(d));
//        for(int i=0;i<d.length;i++){
//            for(int j=0;j<d[i].length;j++){
//                System.out.println();
//            }
//            System.out.println(Arrays.toString(d[i]));
//        mapList = phoenixJdbcdao.queryGas("", "SN1001", "7_1", "2017-01-10 15:14:47", "2017-10-31 16:14:47");
//    }
//            mapList = phoenixJdbcdao.queryWater("340190-05-00-019-000041", "SN1001", "7_1", "2017-10-25 15:14:47", "2017-10-31 16:14:47");
//            mapList = phoenixJdbcdao.queryBridge("00000000", "HF_JZDL_00000003", "1_1", "2017-10-25 15:14:47", "2017-10-25 16:14:47","");
//        System.out.println(mapList);
////        }
//        System.out.println(getPercent(1));
    }
}
