package com.gsafety.bigdata.lifeline.util;

/**
 * @Author: yifeng G
 * @Date: Create in 14:33 2017/8/29 2017
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public interface SystemTypeCode {
    public static final String BRIDGE_EQUIPMENT_TYPE_ACCE = "ODS.BRIDGE_ACCE";//加速度
    public static final String BRIDGE_EQUIPMENT_TYPE_DISPMT = "ODS.BRIDGE_DISPMT";
    public static final String BRIDGE_EQUIPMENT_TYPE_DYNDEF = "ODS.BRIDGE_DYNDEF";
    public static final String BRIDGE_EQUIPMENT_TYPE_STRAIN = "ODS.BRIDGE_STRAIN";
    public static final String BRIDGE_EQUIPMENT_TYPE_LOWSUMARY = "ODS.BRIDGE_LOWSUMARY";
    public static final String BRIDGE_STATUS_TYPE="bridge";
    public static final String GAS_STATUS_TYPE = "gas";
    public static final String WATER_STATUS_TYPE = "water";
    public static final String IOT_LOG_TYPE = "IOT";
    public static final String COMPRESS_PATH = "/lifelineapps/lifeline_dubbo_qc_phoenix/tmp/compress/";//文件压缩后放的目录
    public static final String TARGET_PATH = "/lifelineapps/lifeline_dubbo_qc_phoenix/tmp/target/";//文件缓存的目录
    public static final String ROOT_PATH = "/lifelineapps/lifeline_dubbo_qc_phoenix/tmp/orginal/";//查询后的数据存放的目录
    public static final int COUNT = 0;
}
