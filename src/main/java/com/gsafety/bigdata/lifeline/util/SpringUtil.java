package com.gsafety.bigdata.lifeline.util;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by zdm on 2017/8/14.
 */
public class SpringUtil {
    private static ApplicationContext ac;
    static{
            ac = new ClassPathXmlApplicationContext("applicationContext.xml");
    }

    public static Object getBean(String name){
        return ac.getBean(name);
    }


}
