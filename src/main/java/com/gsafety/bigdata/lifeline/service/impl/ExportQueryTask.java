package com.gsafety.bigdata.lifeline.service.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.gsafety.bigdata.lifeline.pojo.ExportParam;
import com.gsafety.bigdata.lifeline.util.FileUtil;
import com.gsafety.bigdata.lifeline.util.SystemTypeCode;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.gsafety.bigdata.lifeline.util.SplitDateUtil.findDates;

/**
 * @Author: yifeng Ge
 * @Date: Create in 15:33 2017/8/17 2017
 * @Description:通过接口调用查询数据信息到文件然后上传到fastdfs分布式文件系统中返回URL
 * @Modified By:
 * @Vsersion:V1.0
 */
@SuppressWarnings("all")
public class ExportQueryTask {

    private Logger LOG = Logger.getLogger(ExportQueryTask.class);

    private DruidDataSource dataSource;

    public void queryData(ExportParam exportParam) throws Exception {
        /*****************************************************************************************************************************************/
        String originalPath = "";
        String zipPath = "";//压缩的路径
        String backURL = "";//fastdfs返回的url
        String TABLE;
        StringBuilder querySQL = new StringBuilder();
        //"bridgeMonitors01":加速度；"bridgeMonitors10"：位移；"bridgeMonitors13"：动态挠度；"bridgeMonitors02"：应变
        switch (exportParam.getSystemCode()) {
            case SystemTypeCode.BRIDGE_STATUS_TYPE:
                if (exportParam.getEquipmentType().equals("ACCE")) {
                    TABLE = SystemTypeCode.BRIDGE_EQUIPMENT_TYPE_ACCE;
                } else if (exportParam.getEquipmentType().equals("DISPMT")) {
                    TABLE = SystemTypeCode.BRIDGE_EQUIPMENT_TYPE_DISPMT;
                } else if (exportParam.getEquipmentType().equals("DYNDEF")) {
                    TABLE = SystemTypeCode.BRIDGE_EQUIPMENT_TYPE_DYNDEF;
                } else if (exportParam.getEquipmentType().equals("STRAIN")) {
                    TABLE = SystemTypeCode.BRIDGE_EQUIPMENT_TYPE_STRAIN;
                } else {
                    TABLE = SystemTypeCode.BRIDGE_EQUIPMENT_TYPE_LOWSUMARY;
                }
                querySQL.append("SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM " + TABLE + "");
                break;
            case SystemTypeCode.GAS_STATUS_TYPE:
                TABLE = "ODS.GAS";
                querySQL.append("SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM " + TABLE + "");
                break;
            case SystemTypeCode.WATER_STATUS_TYPE:
                TABLE = "ODS.WATER";
                querySQL.append("SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM " + TABLE + "");
                break;
            default:
                break;
        }
        querySQL.append(" WHERE 1=1 ");
        String tempPATH = exportParam.getSystemCode() + "/";
        if (StringUtils.isNotEmpty(exportParam.getLocations())) {
            querySQL.append(" AND LOCATION='" + exportParam.getLocations().trim() + "'");
            tempPATH = tempPATH + exportParam.getLocations() + "-";
        }
        if (StringUtils.isNotEmpty(exportParam.getTerninal())) {
            querySQL.append(" AND TERMINAL='" + exportParam.getTerninal().trim() + "'");
            tempPATH = tempPATH + exportParam.getTerninal() + "-";
        }
        if (StringUtils.isNotEmpty(exportParam.getSensor())) {
            querySQL.append(" AND SENSOR='" + exportParam.getSensor().trim() + "'");
            tempPATH = tempPATH + exportParam.getSensor();
        }
        if (StringUtils.isNotEmpty(exportParam.getStartTime()) && StringUtils.isNotEmpty(exportParam.getEndTime())) {
            String SQLString = "";
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date dBegin = sdf.parse(exportParam.getStartTime().trim());
            Date dEnd = sdf.parse(exportParam.getEndTime().trim());
            List<Date> lDate = findDates(dBegin, dEnd);//对时间段进行按天划分
            ClassPathXmlApplicationContext ct = new ClassPathXmlApplicationContext(new String[]{"applicationProvider.xml"});//测试用，部署之后会删除
            if (null != lDate) {
                for (int i = 0; i < lDate.size(); i++) {
                    /**********查看时间是否在三个月之内的时间********/
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTime(lDate.get(i));
                    calendar.add(calendar.MONTH, -2);
                    System.out.println(sdf.format(calendar.getTime()));
                    originalPath = SystemTypeCode.ROOT_PATH + exportParam.getSystemCode() + "/" + exportParam.getTerninal() + "-" + exportParam.getSensor() + "/" + sdf.format(calendar.getTime()).substring(0, 4) + "/" + sdf.format(calendar.getTime()).substring(5, 7) + "/" + resultMessage(sdf.format(calendar.getTime())) + ".csv";
                    File orgPath = new File(originalPath);
                    if (orgPath.exists()) {
                        orgPath.delete();
                    }
                    /**********************************************/
                    SQLString = querySQL.toString() + " AND  DT  ='" + resultMessage_(sdf.format(lDate.get(i)).toString()) + "'" + " order by times desc";
                    listQuery(SQLString, tempPATH, exportParam.getEquipmentType(), sdf.format(lDate.get(i)).toString());//将文件进行按天划分写入数据
                }
            }
        }
    }

    private void listQuery(String querySQL, String path, String projectName, String Time) {
        long time1 = System.currentTimeMillis();
        String rootPath = "";//根目录
        String targetPath = "";//缓存目录
        ResultSet res = null;
        File fileRootPath = null;
        File fileTargetPath = null;
        try {
            if (dataSource == null) {
                dataSource = new DruidDataSource();
                dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
                dataSource.setUrl("jdbc:hive2://10.5.4.41:10001");
                dataSource.setUsername("");
                dataSource.setPassword("");
                dataSource.setInitialSize(5);
                dataSource.setMinIdle(1);
                dataSource.setMaxActive(20);
                dataSource.setMaxWait(60000);
                dataSource.setMinEvictableIdleTimeMillis(300000);
                dataSource.setPoolPreparedStatements(false);
            }
            Connection conn = dataSource.getConnection();
//            Class.forName("org.apache.hive.jdbc.HiveDriver");
//            Connection conn = DriverManager.getConnection("jdbc:hive2://10.5.4.41:10001", "", "");
            Statement stmt = conn.createStatement();
            long time2 = System.currentTimeMillis();
            System.out.println("连接hive总花销：" + (time2 - time1) / 1000 + "s");
            LOG.info(querySQL);
            System.out.println(querySQL);
            /*************************************************创建根目录文件夹***************************************************************/
            if (FileUtil.createFileDirectory(SystemTypeCode.ROOT_PATH)) {//防止根文件夹被误删除
                if (projectName.equals("")) {
                    rootPath = SystemTypeCode.ROOT_PATH + path + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                    rootPath.replace("//", "/");
                } else {
                    rootPath = SystemTypeCode.ROOT_PATH + path + "/" + projectName + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                    rootPath.replace("//", "/");
                }
                fileRootPath = new File(rootPath);
                if (!fileRootPath.getParentFile().exists()) {
                    fileRootPath.getParentFile().mkdirs();
                }
                fileRootPath.createNewFile();
            }
            FileWriter fw = new FileWriter(rootPath);//如果是target是运维支撑平台查询环境，如果是rootPath则是定时器开启的环境
            try {
                long time3 = System.currentTimeMillis();
                conn.setAutoCommit(false);
                stmt.execute("set spark.sql.thriftServer.exportCollect=true");
                res = stmt.executeQuery(querySQL);
                long time4 = System.currentTimeMillis();
                System.out.println("查询总开销是：" + (time4 - time3) / 1000 + "s");
                ResultSetMetaData md = res.getMetaData();
                int columnCount = md.getColumnCount();
                StringBuilder stringBuilder = new StringBuilder();
                int count = 0;
                while (null != res && res.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        if (res.getString(5).length() < 1000) {//去除傅里叶变换的值
                            stringBuilder.append(formatStr(res.getString(i), 20));
                        }
                    }
                    stringBuilder.append("\n");
                    count++;
                    if (count % 1000000 == 0) {
                        fw.write(stringBuilder.toString());
                        stringBuilder.setLength(0);
                    }
                }
                long time5 = System.currentTimeMillis();
                System.out.println("遍历总开销：" + (time5 - time4) / 1000 + "s");
                /*********************************************写入文件***********************************************/
                fw.write(stringBuilder.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                fw.flush();
                fw.close();
                res.close();
                stmt.close();
                conn.close();
            }
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));
            String str = sw.toString();
            System.out.println("==========");
            System.out.println(str);
            LOG.error("异常信息捕获中................" + str);
        }
    }

    /**
     * 格式化数据
     *
     * @param str
     * @param length
     * @return
     */

    public String formatStr(String str, int length) {
        str = "  " + str;
        int strLen = str.getBytes().length;
        if (strLen < length) {
            int temp = length - strLen;
            for (int i = 0; i < temp; i++) {
                str += " ";
            }
        }
        return str;
    }

    /**
     * 格式化文件名
     *
     * @param strMessage
     * @return
     */
    public String resultMessage(String strMessage) {
        return strMessage.replace("-", "").replace(":", "").replace(" ", "");
    }

    /**
     * 格式化分区表字段
     *
     * @param message
     * @return
     */
    public String resultMessage_(String message) {
        return message.replace("-", "").substring(0, 8);
    }

}

























