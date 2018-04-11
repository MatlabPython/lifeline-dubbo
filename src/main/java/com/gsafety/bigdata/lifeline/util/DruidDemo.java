package com.gsafety.bigdata.lifeline.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: yifeng G
 * @Date: Create in 20:24 2018/3/8 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class DruidDemo {
    @Test
    public void druidTest(){
        Connection conn = null;
//        PreparedStatement pstmt = null;
        ResultSet rs = null;
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setUrl("jdbc:hive2://10.5.4.41:10001");
        dataSource.setUsername("");
        dataSource.setPassword("");
        try{
            // 获得连接:
            conn = dataSource.getConnection();
            // 编写SQL：
            String sql = "SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM ODS.GAS LIMIT 10";
            Statement stmt = conn.createStatement();
            // 执行sql:
            rs = stmt.executeQuery(sql);
            while(rs.next()){
                System.out.println(rs.getString("VALUES")+"   "+rs.getString("TIMES"));
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
//            Utils.releaseResouce(rs, pstmt, conn);
        }

    }
}
