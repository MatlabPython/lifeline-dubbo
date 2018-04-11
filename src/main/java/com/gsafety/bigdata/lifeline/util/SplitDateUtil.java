package com.gsafety.bigdata.lifeline.util;

import com.gsafety.bigdata.lifeline.pojo.KeyValueForDate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @Author: yifeng G
 * @Date: Create in 14:26 2017/8/17 2017
 * @Description:对跨月进行拆分
 * @Modified By:
 * @Vsersion:v1.0
 */
public class SplitDateUtil {
//    @Test
//    public void demo() {
//        List<KeyValueForDate> list = SplitDateUtil.getKeyValueForDate("2016-08-23", "2016-09-30");
//        System.out.println("开始日期--------------结束日期");
//        for (KeyValueForDate date : list) {
//            System.out.println(date.getStartDate() + "-----" + date.getEndDate());
//        }
//    }

    /**
     * 根据一段时间区间，按月份拆分成多个时间段
     *
     * @param startDate 开始日期
     * @param endDate   结束日期
     * @return
     */
    @SuppressWarnings("deprecation")
    public static List<KeyValueForDate> getKeyValueForDate(String startDate, String endDate) {
        List<KeyValueForDate> list = null;
        try {
            list = new ArrayList<KeyValueForDate>();
            String firstDay = "";
            String lastDay = "";
            Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);// 定义起始日期
            Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);// 定义结束日期
            Calendar dd = Calendar.getInstance();// 定义日期实例
            dd.setTime(d1);// 设置日期起始时间
            Calendar cale = Calendar.getInstance();
            Calendar c = Calendar.getInstance();
            c.setTime(d2);
            int startDay = d1.getDate();
            int endDay = d2.getDate();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            KeyValueForDate keyValueForDate = null;
            while (dd.getTime().before(d2)) {// 判断是否到结束日期
                keyValueForDate = new KeyValueForDate();
                cale.setTime(dd.getTime());
                if (dd.getTime().equals(d1)) {
                    cale.set(Calendar.DAY_OF_MONTH, dd.getActualMaximum(Calendar.DAY_OF_MONTH));
                    lastDay = sdf.format(cale.getTime());
                    keyValueForDate.setStartDate(sdf.format(d1));
                    keyValueForDate.setEndDate(lastDay);

                } else if (dd.get(Calendar.MONTH) == d2.getMonth() && dd.get(Calendar.YEAR) == c.get(Calendar.YEAR)) {
                    cale.set(Calendar.DAY_OF_MONTH, 1);//取第一天
                    firstDay = sdf.format(cale.getTime());
                    keyValueForDate.setStartDate(firstDay);
                    keyValueForDate.setEndDate(sdf.format(d2));

                } else {
                    cale.set(Calendar.DAY_OF_MONTH, 1);//取第一天
                    firstDay = sdf.format(cale.getTime());
                    cale.set(Calendar.DAY_OF_MONTH, dd.getActualMaximum(Calendar.DAY_OF_MONTH));
                    lastDay = sdf.format(cale.getTime());
                    keyValueForDate.setStartDate(firstDay);
                    keyValueForDate.setEndDate(lastDay);
                }
                list.add(keyValueForDate);
                dd.add(Calendar.MONTH, 1);// 进行当前日期月份加1
            }

            if (endDay < startDay) {
                keyValueForDate = new KeyValueForDate();
                cale.setTime(d2);
                cale.set(Calendar.DAY_OF_MONTH, 1);//取第一天
                firstDay = sdf.format(cale.getTime());
                keyValueForDate.setStartDate(firstDay);
                keyValueForDate.setEndDate(sdf.format(d2));
                list.add(keyValueForDate);
            }
        } catch (ParseException e) {
            return null;
        }
        return list;
    }

    /**
     * 将时间进行按天划分
     * @param dBegin
     * @param dEnd
     * @return
     */

    public static List<Date> findDates(Date dBegin, Date dEnd) {
        List lDate = new ArrayList();
        lDate.add(dBegin);
        Calendar calBegin = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calBegin.setTime(dBegin);
        Calendar calEnd = Calendar.getInstance();
        // 使用给定的 Date 设置此 Calendar 的时间
        calEnd.setTime(dEnd);
        // 测试此日期是否在指定日期之后
        while (dEnd.after(calBegin.getTime())) {
            // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
            calBegin.add(Calendar.DAY_OF_MONTH, 1);
            lDate.add(calBegin.getTime());
        }
        return lDate;
    }

    public static void main(String[] args) throws Exception{
        Calendar cal = Calendar.getInstance();
        String start = "2012-12-29";
        String end = "2012-12-29";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dBegin = sdf.parse(start);
        Date dEnd = sdf.parse(end);
        List<Date> lDate = findDates(dBegin, dEnd);
        for (Date date : lDate) {
            System.out.println(sdf.format(date));
        }

    }
}

