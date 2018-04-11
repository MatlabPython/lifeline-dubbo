package com.gsafety.bigdata.lifeline.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zhangdm on 2017/11/9.
 */
public class SplitTimeUtil {

    public static void main(String[] args) throws ParseException {
        split("2017-11-05 16:14:47","2017-11-06 16:14:46",3);
    }

    public static List<String> split(String startTime, String endTime,int n) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            Date sTime = dateFormat.parse(startTime);
            Date eTime = dateFormat.parse(endTime);
            long dif = (eTime.getTime() - sTime.getTime())/n;
            ArrayList<String> times = new ArrayList<>();
            for (int i=0;i<n;i++){
                long difTime = sTime.getTime() + dif * i;
                times.add(dateFormat.format(new Date(difTime)));
            }
            times.add(dateFormat.format(eTime));
            System.out.println(times);
            return times;
        }catch (Exception e){
            
        }
        return null;
    }
}
