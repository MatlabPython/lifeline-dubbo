package com.gsafety.bigdata.lifeline.util;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @Author: yifeng G
 * @Date: Create in 13:22 2018/3/1 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class test extends Thread {
    public static void main(String[] args) {
        /**
         * 测试文件之间的copy
         */
//        long time1=System.currentTimeMillis();
//        FileUtil.BufferReaderBufferWriter("E:\\lifelineapps\\lifeline_dubbo_qc_phoenix\\tmp\\orginal\\340122-04-00-001-000125-HF_G206_00000001-10_9(20180301-20180305)\\bridge\\ACCE\\2018\\03\\20180303.csv", "F:\\1.csv");
//        long time2=System.currentTimeMillis();
//        System.out.println("总花销："+(time2-time1)/1000+"s");
        /**
         * 测试定时器
         */
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"applicationProvider.xml"});
        context.start();
        /**
         * 时间间隔测试
         */
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        Calendar calendar=Calendar.getInstance();
//        calendar.setTime(new Date());
//        calendar.add(calendar.MONTH,-3);
//        System.out.println(sdf.format(calendar.getTime()));

    }

}
