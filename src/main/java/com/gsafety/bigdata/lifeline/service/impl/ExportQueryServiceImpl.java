package com.gsafety.bigdata.lifeline.service.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.io.Files;
import com.gsafety.bigdata.lifeline.pojo.ExportParam;
import com.gsafety.bigdata.lifeline.pojo.ResultMessage;
import com.gsafety.bigdata.lifeline.service.ExportQueryService;
import com.gsafety.bigdata.lifeline.util.FileUtil;
import com.gsafety.bigdata.lifeline.util.SystemTypeCode;
import com.gsafety.bigdata.lifeline.util.ZipCompressorUtil;
import com.gszone.lifeline.bscplt.ishare.coresvc.cpnts.dfs.DfsFileOperService;
import com.gszone.lifeline.bscplt.ishare.coresvc.cpnts.dfs.FastDFSTemplate;
import com.gszone.lifeline.bscplt.ishare.coresvc.cpnts.dfs.FastDFSTemplateFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import sun.jvmstat.monitor.MonitorException;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import static com.gsafety.bigdata.lifeline.util.SplitDateUtil.findDates;

/**
 * @Author: yifeng Ge
 * @Date: Create in 15:33 2017/8/17 2017
 * @Description:通过接口调用查询数据信息到文件然后上传到fastdfs分布式文件系统中返回URL
 * @Modified By:
 * @Vsersion:V1.3
 */
@SuppressWarnings("all")
public class ExportQueryServiceImpl implements ExportQueryService {

    private Logger LOG = Logger.getLogger(ExportQueryServiceImpl.class);

    private DruidDataSource dataSource;

    public Map<String, String> queryData(ExportParam exportParam) throws Exception {
        /********************************************将线程的pid存入zookeeper上********************************************************************/
//        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        //获取java执行线程的pid
//        BaseZookeeper baseZookeeper = new BaseZookeeper();
//        baseZookeeper.connectZookeeper(ZK_HOST);
//        String path = "/ThreadController/" + exportParam.getSystemCode() + "/" + exportParam.getLocations() + ":" + exportParam.getTerninal() + ":" + exportParam.getSensor() + ":" + exportParam.getStartTime() + ":" + exportParam.getEndTime();
//        Map<String, String> map = new HashMap<>();
//        map.put("threadExcuter", "{\"theadName\":\"" + Thread.currentThread().getName() + "\"}");
//        map.put("threadStatus", "{\"status\":\"" + exportParam.getStatus() + "\"}");
//        map.put("threadTaskNo", "{\"taskNo\":\"" + exportParam.getTaskNo() + "\"}");
//        if (baseZookeeper.existsNode(path)) {//存在路径就更新znode上的节点数据
//            JSONObject jsonObject = JSONObject.fromObject(baseZookeeper.getData(path));
//            if (exportParam.getStatus().equals("stop")) {//如果是执行stop的线程则kill对应线程
//                Thread thread = getThreadByName(JSONObject.fromObject(jsonObject.getString("threadExcuter")).getString("theadName").toString());
//                baseZookeeper.deleteSubNode(path);
//                System.out.println("线程2--------------->" + thread);
//                thread.interrupt();
//                map.clear();
//                map.put("message", "已停止执行----->" + JSONObject.fromObject(jsonObject.get("threadTaskNo")).get("taskNo") + "<------线程");
//                return map;
//            }
//        } else {
//            baseZookeeper.createNode(path, com.alibaba.fastjson.JSONObject.toJSONString(map));
//        }
        //启动znode节点监听
//        BaskZookeeperEvent baskZookeeperEvent = new BaskZookeeperEvent(ZK_HOST);
//        baskZookeeperEvent.watcherEvent("/test/bridge");
        /*****************************************************************************************************************************************/
        String zipPath = "";
        String backURL = "";
        String TABLE;
        StringBuilder querySQL = new StringBuilder();
        final Map cachemap = new HashMap();
        //"bridge":表示桥梁；"gas":表示燃气；"water":表示供水；其他
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
//                WRList.add(strFormate("传感器定位,终端号,传感器编号,监测值,数据采集时间") + "\n");
                break;
            case SystemTypeCode.GAS_STATUS_TYPE:
                TABLE = "ODS.GAS";
                querySQL.append("SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM " + TABLE + "");
//                WRList.add(strFormate("传感器定位,终端号,传感器编号,监测值,数据采集时间,电压,状态标识") + "\n");
                break;
            case SystemTypeCode.WATER_STATUS_TYPE:
                TABLE = "ODS.WATER";
                querySQL.append("SELECT LOCATION,TERMINAL,SENSOR,MONITORING,VALUES,FROM_UNIXTIME(CAST(CAST(TIME AS  DOUBLE)/1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss')AS TIMES FROM " + TABLE + "");
//                WRList.add(strFormate("传感器定位,终端号,传感器编号,监测值,数据采集时间,信号强度") + "\n");
                break;
            case SystemTypeCode.IOT_LOG_TYPE:
                TABLE = "KafkaToEs";
                querySQL.append("SELECT * FROM " + TABLE + "");
                break;
            default:
                break;
        }
        querySQL.append(" WHERE 1=1 ");
        String tempPATH = exportParam.getSystemCode() + "/";
        if (StringUtils.isNotEmpty(exportParam.getLocations())) {
            querySQL.append(" AND LOCATION='" + exportParam.getLocations().trim() + "'");
            tempPATH = tempPATH + exportParam.getLocations().trim() + "-";
        }
        if (StringUtils.isNotEmpty(exportParam.getTerninal())) {
            querySQL.append(" AND TERMINAL='" + exportParam.getTerninal().trim() + "'");
            tempPATH = tempPATH + exportParam.getTerninal().trim() + "-";
        }
        if (StringUtils.isNotEmpty(exportParam.getSensor())) {
            querySQL.append(" AND SENSOR='" + exportParam.getSensor().trim() + "'");
            tempPATH = tempPATH + exportParam.getSensor().trim();
        }
//        tempPATH = tempPATH + "(" + resultMessage(exportParam.getStartTime()) + "-" + resultMessage(exportParam.getEndTime()) + ")";
//        if (StringUtils.isNotEmpty(exportParam.getEquipmentType())) {
//            querySQL.append(" AND (MONITORING='" + exportParam.getEquipmentType() + "' or MONITORING='" + exportParam.getEquipmentType().toLowerCase() + "')");
//        }
        if (StringUtils.isNotEmpty(exportParam.getStartTime()) && StringUtils.isNotEmpty(exportParam.getEndTime())) {
            String SQLString = "";
//            if (exportParam.getSystemCode().equals(SystemTypeCode.BRIDGE_STATUS_TYPE)) {
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date dBegin = sdf.parse(exportParam.getStartTime().trim());
            Date dEnd = sdf.parse(exportParam.getEndTime().trim());
            List<Date> lDate = findDates(dBegin, dEnd);//对时间段进行按天划分
            if (null != lDate) {
                for (int i = 0; i < lDate.size(); i++) {
//                        if (exportParam.getSystemCode().equals(SystemTypeCode.IOT_LOG_TYPE)) {//查询采集平台的日志信息
//                            SQLString = querySQL.toString();
//                        } else {
                    SQLString = querySQL.toString() + " AND  DT  ='" + resultMessage_(sdf.format(lDate.get(i)).toString()) + "'" + " order by times desc";
//                        }
                    listQuery(SQLString, tempPATH, exportParam.getEquipmentType().trim(), sdf.format(lDate.get(i)).toString());//将文件进行按天划分写入数据
                }

            }
//            }
        }
//        else if () {
//            cachemap.put("message", "查询超时");
//            cachemap.put("taskStatus", "fail");
//        }
        else {
            FileUtil.nioTransferCopy(new File(SystemTypeCode.ROOT_PATH + exportParam.getSystemCode()), new File(SystemTypeCode.TARGET_PATH + exportParam.getSystemCode()));
        }
        /*********************************压缩文件**********************************************************/
        if (FileUtil.createFileDirectory(SystemTypeCode.COMPRESS_PATH)) {
            zipPath = SystemTypeCode.COMPRESS_PATH + tempPATH + ".zip";
        }
        File compressFile = new File(zipPath);
        if (!compressFile.getParentFile().exists()) {
            compressFile.getParentFile().mkdirs();
        }
        compressFile.createNewFile();
//                if (!compressFile.exists()) {
//                    compressFile.createNewFile();
//                }
        ZipCompressorUtil zipCompressor = new ZipCompressorUtil(zipPath);
        zipCompressor.compress(SystemTypeCode.TARGET_PATH + tempPATH);
        /*******************************上传fastdfs分布式文件系统************************************************/
        Map<String, String> keyValues = new HashMap<String, String>();
//                  keyValues.put(DfsFileOperService.META_APPFILENAME, Timestamp.valueOf(startTime).getTime() + "-" + Timestamp.valueOf(endTime).getTime() + ".zip");
        keyValues.put(DfsFileOperService.META_APPFILENAME, tempPATH + ".zip");
        byte[] fbuf = Files.toByteArray(new File(zipPath));
        ClassPathXmlApplicationContext ct = new ClassPathXmlApplicationContext(new String[]{"applicationProvider.xml"});//测试用，部署之后会删除
        FastDFSTemplateFactory fastDFSTemplateFactory = (FastDFSTemplateFactory) ct.getBean("fastDFSTemplateFactory");
        FastDFSTemplate fastDFSTemplate = new FastDFSTemplate(fastDFSTemplateFactory);
        if (null == fastDFSTemplate) {
            ResultMessage.getMessageFromCode("501").getMessage();
        }
        backURL = fastDFSTemplate.upload(fbuf, zipPath.split("\\.")[1], keyValues).getWebPath() == null ? "" : fastDFSTemplate.upload(fbuf, zipPath.split("\\.")[1], keyValues).getWebPath();
        File file = new File(zipPath);
        cachemap.put("zipSIZE", file.length() + "B");
        cachemap.put("zipPATH", backURL);
        cachemap.put("taskNo", exportParam.getTaskNo());
        cachemap.put("taskStatus", "success");
//                map.put("threadStatus", "{\"taskStatus\":\"" + "success" + "\"}");
//                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                map.put("threadUpdateDate", simpleDateFormat.format(new Date()));
//                if (baseZookeeper.existsNode(path)) {
//                    baseZookeeper.setData(path, com.alibaba.fastjson.JSONObject.toJSONString(map));
//                } else {
//                    baseZookeeper.createNode(path, com.alibaba.fastjson.JSONObject.toJSONString(map));
//                }
        /**
         * 上传fastDFS后删除压缩的文件以及源文件以释放磁盘空间
         */
        FileUtil.deletefile(zipPath);
        FileUtil.deletefile(SystemTypeCode.TARGET_PATH + tempPATH);
        System.out.println(cachemap);
        return cachemap;
    }

    private void listQuery(String querySQL, String path, String projectName, String Time) {
        long time1 = System.currentTimeMillis();
        String rootPath = "";//根目录
        String targetPath = "";//缓存目录
        ResultSet res = null;
        File fileRootPath = null;
        File fileTargetPath = null;
        try {
            /*************************************************删除超过三个月的数据节省磁盘空间***************************************************/


            /*************************************************创建缓存目录文件夹***************************************************************/
            if (FileUtil.createFileDirectory(SystemTypeCode.TARGET_PATH)) {//防止缓存文件夹被误删除
                if (StringUtils.isEmpty(projectName)) {
                    targetPath = SystemTypeCode.TARGET_PATH + path + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                } else {
                    targetPath = SystemTypeCode.TARGET_PATH + path + "/" + projectName + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                }
                fileTargetPath = new File(targetPath);
                if (!fileTargetPath.getParentFile().exists()) {
                    fileTargetPath.getParentFile().mkdirs();
                }
                fileTargetPath.createNewFile();
            }
            /*************************************************创建根目录文件夹***************************************************************/
            if (FileUtil.createFileDirectory(SystemTypeCode.ROOT_PATH)) {//防止根文件夹被误删除
                if (StringUtils.isEmpty(projectName)) {
                    rootPath = SystemTypeCode.ROOT_PATH + path + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                    rootPath.replace("//", "/");
                } else {
                    rootPath = SystemTypeCode.ROOT_PATH + path + "/" + projectName + "/" + Time.substring(0, 4) + "/" + Time.substring(5, 7) + "/" + resultMessage(Time) + ".csv";
                    rootPath.replace("//", "/");
                }
            }
            File file = new File(rootPath);
            if (file.exists()) {
                FileUtil.nioTransferCopy(new File(rootPath), new File(targetPath));
            } else {

                /**************************************************引入缓冲池******************************************************************************/
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
                /***********************************************************************************************************************************************************************/
//                Class.forName("org.apache.hive.jdbc.HiveDriver");
//                Connection conn = DriverManager.getConnection("jdbc:hive2://10.5.4.41:10001", "", "");
                Statement stmt = conn.createStatement();
                long time2 = System.currentTimeMillis();
                System.out.println("连接hive总花销：" + (time2 - time1) / 1000 + "s");
                LOG.info(querySQL);
                System.out.println(querySQL);
                FileWriter fw = new FileWriter(targetPath);
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
            }

            /******************************将文件打散成多个小文件，防止文件过大而打不开***********************************************/
//            FileUtil fileUtil = new FileUtil();
//            float filesize = fileUtil.getfileSize(file.getPath());
//            System.out.println(filesize);
//            if (filesize > blockFileSize) {//如果大于100M就进行文件拆分处理，否则就不做处理
//                fileUtil.splitBySize(file.getPath(), blockFileSize);
//                long tstart = System.currentTimeMillis();
//                LOG.info("starting while...........");
//                boolean flag = true;
//                while (flag) {
//                    if (fileUtil.countThread == 0) {
//                        flag = false;
//                        Thread.sleep(100L);
//                    }
//                    LOG.info(fileUtil.countThread);
//                }
//                LOG.info("ending while.................");
//            }
//            file.delete();
//            long time6 = System.currentTimeMillis();
//          System.out.println("写入文本总花销：" + (time6 - tstart) / 1000 + "s");
//
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
     * 格式化表头
     *
     * @param str
     * @return
     */
    public String strFormate(String str) {
        String strMessage = "";
        String[] stringArr = str.split(",");
        for (int i = 0; i < stringArr.length; i++) {
            strMessage = strMessage + formatStr(stringArr[i], 25);
        }
        return strMessage;
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

    /**
     * 将ResultSet的对象转换成list
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private static List convertList(ResultSet rs) throws SQLException {
        List list = new ArrayList();
        ResultSetMetaData md = rs.getMetaData();//获取键名
        int columnCount = md.getColumnCount();//获取行的数量
        while (rs.next()) {
            Map rowData = new HashMap();//声明Map
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));//获取键名及值
            }
            list.add(rowData);
        }
        return list;
    }

    /**
     * @param threadName
     * @return
     */
    public Thread getThreadByName(String threadName) {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals(threadName)) return thread;
        }
        return null;
    }

    public static void main(String[] args) throws MonitorException, URISyntaxException {
        System.out.println("哈哈....");
    }
}

























