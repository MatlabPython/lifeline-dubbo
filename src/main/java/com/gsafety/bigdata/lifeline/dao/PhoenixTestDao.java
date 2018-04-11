package com.gsafety.bigdata.lifeline.dao;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.MD5Hash;
//import org.apache.phoenix.shaded.org.junit.Test;
import org.jboss.logging.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/9/6.
 */
@SuppressWarnings("all")
public class PhoenixTestDao {
    private static JdbcTemplate template;
    private static PhoenixMDao phoenixDao;
    private Logger logger = Logger.getLogger(PhoenixTestDao.class);

    static {
        ApplicationContext ac = new ClassPathXmlApplicationContext("phoenix-jdbc.xml");
        template = ac.getBean("phoenixJdbcTemplate", JdbcTemplate.class);
//        phoenixDao = ac.getBean("phoenixMDao", PhoenixMDao.class);
    }

//    @Resource(name="jdbcTemplatePhoenix")
//    JdbcTemplate template;
//    where "PK" BETWEEN '02a875:00000000:HF_PHDQ_00000001:32_8:9223370528209074807:01 ' AND '02a875:00000000:HF_PHDQ_00000001:32_8:9323370528209074807:01' and "dataType" = 'S10M'
    public void test() {
//        String sql = "select * from BRIDGE_HF_JZDL_00000003_1s where 1=1 and \"level\" BETWEEN -1 AND 2";
        String sql = "select * from BRIDGE_HF_JZDL_00000003_1s limit 10";
//        String sql = "select * from BRIDGE_FEATURE_10M limit 10";
        List<Map<String, Object>> maps = template.queryForList(sql);
        System.out.println(maps);
    }

    public static void main(String[] args) {
        new PhoenixTestDao().test();
//        new PhoenixTestDao().queryBridge("00000000", "HF_JZDL_00000003", "4_4", "2017-01-06 16:00:00", "2017-09-06 16:59:00");
//        System.out.println(phoenixDao.selectLimit("BRIDGE_HF_JZDL_00000003_1s",100000));
//        System.out.println(phoenixDao.selectLimit("BRIDGE_HF_JZDL_00000003_1s",100000));
    }

    public List<Object> queryBridge(String locations, String terninal, String sensor, String starTime, String endTime) {

        try {

            String table = "bridge_" + terninal + "_1S";

            return query(locations, terninal, sensor, starTime, endTime, table);

        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }


    public List<Object> query(String locations, String terninal, String sensor, String starTime, String endTime,
                              String table) {
        ResultScanner res = null;
        List<Object> lists = new ArrayList<Object>();
        try {
            SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            String md5String = sensor + terninal + locations;

            String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            long start = dateFormat.parse(starTime).getTime();
            long end = dateFormat.parse(endTime).getTime();

            String startrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
                    + (Long.MAX_VALUE - end) + ":00";

            String endrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
                    + (Long.MAX_VALUE - start) + "00";

            logger.info("Startrow :" + startrowkey);
            logger.info("endrow :" + endrowkey);
            long t1 = System.currentTimeMillis();

            String baseSql = "select \"rtime\" as \"time\",\"dataType\",\"level\",round(to_number(\"values\"),2) as \"values\" from " + table + " where 1=1";
            StringBuffer stringBuffer = new StringBuffer(baseSql);
            stringBuffer.append(" and \"PK\" between \'" + startrowkey + "\' and \'" + endrowkey+"\'");
//            stringBuffer.append(" and \"time\" between " + starTime + " and " + endTime);
//            stringBuffer.append(" and \"level\" between " + -1 + " and " + 2);
            List<Map<String, Object>> maps = template.queryForList(stringBuffer.toString());
            System.out.println(maps);
        }
            catch (Exception e) {
            logger.error("", e);
        } finally {

        }
        return null;
    }

}
