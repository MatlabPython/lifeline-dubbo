package com.gsafety.bigdata.lifeline.util;

import com.gsafety.bigdata.lifeline.pojo.ExportParam;
import com.gsafety.bigdata.lifeline.service.impl.ExportQueryTask;
import com.lifeline.bigdata.lifeline.util.redis.JedisUtil;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: yifeng G
 * @Date: Create in 9:52 2018/3/1 2018
 * @Description:定时器将hive数据持久化到本地磁盘
 * @Modified By:yifeng G
 * @Vsersion:v1.0.0
 */
@Component
@EnableScheduling
public class TaskJobSchedu {

    private static Logger LOG = Logger.getLogger(TaskJobSchedu.class);

//    @Scheduled(cron = "0 0 0 * * ?")//每天的24点执行一次（0 0 0 * * ?）,每隔两秒执行一次（0/2 * * * * ?）
    public static void job() {
        long time0 = System.currentTimeMillis();
        ExportParam exportParam = new ExportParam();
        ExportQueryTask exportQueryTask = new ExportQueryTask();
        int[] index = {1, 2, 3, 4, 5};//1桥梁专项，2供水专项，3燃气专项，4热力专项，5排水专项
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        List list = new ArrayList();
        for (int i = 1; i < 6; i++) {
            if (index[1] == i) {//供水专项redis里的terminal+sensor的配置
                list = JedisUtil.getAllKeys(2);
                exportParam.setSystemCode("water");
                exportParam.setEquipmentType("");
                exportParam.setTaskNo("water" + i);

            } else if (index[2] == i) {//燃气专项redis里的terminal+sensor的配置
                list = JedisUtil.getAllKeys(3);
                exportParam.setSystemCode("gas");
                exportParam.setEquipmentType("");
                exportParam.setTaskNo("gas" + i);

            }
            if (list != null) {
                for (int j = 1; j < list.size(); j++) {
                    String tempStr = list.get(j).toString();
                    String terminalLast = tempStr.substring(5, tempStr.lastIndexOf("_"));
                    exportParam.setTerninal(terminalLast.substring(0, terminalLast.lastIndexOf("_")));
                    String sensorStr = tempStr.substring(0, tempStr.lastIndexOf("_"));
                    exportParam.setSensor(tempStr.substring(sensorStr.lastIndexOf("_") + 1, tempStr.length() - 1));
                    exportParam.setStatus("running");
                    Calendar cdr = Calendar.getInstance();
                    cdr.add(Calendar.DATE, -2);
                    exportParam.setEndTime(sdf.format(cdr.getTime()));
                    exportParam.setStartTime(sdf.format(cdr.getTime()));
//                    exportParam.setEndTime("2018-03-13");
//                    exportParam.setStartTime("2018-03-13");
                    try {
                        exportQueryTask.queryData(exportParam);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        long time1 = System.currentTimeMillis();
        System.out.println("***************************************************");
        System.out.println("  ***********************************************");
        System.out.println("   ********************************************");
        System.out.println("      *********************************");
        System.out.println("      总花销需:" + (time1 - time0) / 1000 + "s");
        System.out.println("      **********************************");
        System.out.println("   *********************************************");
        System.out.println("  ************************************************");
        System.out.println("*****************************************************");
        LOG.info("定时器启动时间------------>" + sdf.format(new Date()));
    }

    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cdr = Calendar.getInstance();
        cdr.add(Calendar.DATE, -3);
        System.out.println(sdf.format(cdr.getTime()));
//        int[] index = {0, 1, 2};
//        System.out.println(index[0]);
//        String str = "WSD-[000107_0_12]";
//        String terminalLast = str.substring(5, str.lastIndexOf("_"));
//        System.out.println("terminal------>" + terminalLast.substring(0, terminalLast.lastIndexOf("_")));
//        String secondStr = str.substring(0, str.lastIndexOf("_"));
//        System.out.println(str.substring(secondStr.lastIndexOf("_") + 1, str.length() - 1));
        job();
    }
}


