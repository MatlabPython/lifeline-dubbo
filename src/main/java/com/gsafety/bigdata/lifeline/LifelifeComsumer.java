package com.gsafety.bigdata.lifeline;

import com.gsafety.bigdata.lifeline.pojo.ExportParam;
import com.gsafety.bigdata.lifeline.service.ExportQueryService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map;

public class LifelifeComsumer {

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"applicationConsumer.xml"});
//        BridgeQueryService brService = (BridgeQueryService) context.getBean("BridgeQueryService");
//        WaterQueryService waService = (WaterQueryService) context.getBean("WaterQueryService");
//        GasQueryService gasService = (GasQueryService) context.getBean("GasQueryService");
        ExportQueryService exportQueryService = (ExportQueryService) context.getBean("ExportQueryService");
        /**************************************************桥梁调试*****************************************************/
//        int i = 0;
//        while (true) {
//            BridgeParam bridgeParam = new BridgeParam("00000000", "HF_FHDD_DY000001", "17_1", "2017-06-01 20:00:00", "2017-09-09 23:59:00", "S10M");
//            String jsonBridge = brService.queryBridge(bridgeParam);
////            String jsonBridge2 = brService.queryBridgeFeature(bridgeParam);
//            System.out.println(jsonBridge);
//            i++;
//            System.out.println(i);
//        }
//        while(true){
//            String json2 = brService.queryBridgeEnviroment(new String[]{"3110JZHSBGKD3"}, "402884405026354d6005", "20170501", "20170901");
//            System.out.println("查询所需要的数据----------->"+json2);
//        }

//        /****************************************************燃气调试****************************************************/
//        int i = 0;
//        while (true) {
//            GasParam gas = new GasParam("00000000","NDIR001710090091","0_0","2018-01-29 18:00:41","2018-01-29 19:00:41");
//            String json = gasService.queryGas(gas);
//            System.out.println("gas result:" + json);
//            i++;
//            System.out.println(i);
//        }

        /**************************************************供水调试********************************************************/
//        WaterParam waterParam = new WaterParam("340190-05-00-017-000044", "000121", "0_12", "2017-05-25 18:00:00:00", "2017-05-25 18:00:00:00","S");
//         WaterParam waterParam = new WaterParam("00000000", "000121", "0_12", "2017-09-25 18:00:00:00", "2017-09-25 18:00:00:00","S");
//         String jsonWater = waService.queryWaterFlow(waterParam);
//         System.out.println(jsonWater);
        /*************************************************大数据导出调试**************************************************/
        //桥梁导出测试
//        ExportParam param = new ExportParam("340122-04-00-001-000172","HF_PHDQ_00000001", "1_1", "2017-11-01", "2017-11-10", "bridge","ACCE","A343433","runnning");
//        燃气导出测试
//        ExportParam param = new ExportParam("",
//                "NDIR00175", "0_0", "2018-01-01 00:00:00", "2018-03-05 00:00:00", "gas",
//                "","gas00001","1");
        ExportParam param = new ExportParam("",
                "NDIR001711140291", "0_0", "2000-01-01", "2000-02-01", "gas",
                "","gas00001","1");
//        ExportParam param = new ExportParam("", "000137", "0_12", "2018-01-01", "2018-03-12", "water", "","gas00001","running");  // 供水专项测试
        Map<String, String> map=exportQueryService.queryData(param);
        System.out.println(exportQueryService.queryData(param));
        System.out.println("导出成功.........");
    }
}
