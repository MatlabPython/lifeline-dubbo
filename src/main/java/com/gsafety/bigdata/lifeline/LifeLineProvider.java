package com.gsafety.bigdata.lifeline;

import java.io.File;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class LifeLineProvider {
    private static Logger logger = Logger.getLogger(LifeLineProvider.class);

    public static void main(String[] args) throws Exception {
        // System.setProperty("zookeeper.sasl.client", "false");
        String conf_home = System.getenv("CONF_HOME");
        logger.info("CONF_HOME :" + conf_home);
        if (conf_home == null) {
            conf_home="F:\\大数据N\\lifeline-dubbo\\conf\\";
            throw new Exception("CONF_HOME env not exits");
        }
        if (conf_home.endsWith("/")) {
            conf_home = conf_home.substring(0, conf_home.length() - 2);
        }
        PropertyConfigurator.configure(conf_home + File.separator + "log4j.properties");
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"hive-jdbc.xml","phoenix-jdbc.xml","applicationProvider.xml"});
        context.start();
        logger.info("dubbo provider is running...");
        com.alibaba.dubbo.container.Main.main(args);
    }
}
