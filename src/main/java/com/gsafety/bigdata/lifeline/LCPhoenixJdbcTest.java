package com.gsafety.bigdata.lifeline;

import com.gsafety.bigdata.lifeline.pojo.HeatParam;
import com.gsafety.bigdata.lifeline.service.HeatQueryService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by Administrator on 2017/9/7.
 */
public class LCPhoenixJdbcTest {
    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[]{"applicationConsumer.xml"});
//        BridgeQueryService brService = (BridgeQueryService) context.getBean("BridgeQueryService");
//       GasQueryService gasQueryService = (GasQueryService) context.getBean("GasQueryService");
//        WaterQueryService waterQueryService = (WaterQueryService) context.getBean("WaterQueryService");
//        DrainQueryService drainQueryService = (DrainQueryService) context.getBean("DrainQueryService");
        HeatQueryService heatQueryService= (HeatQueryService) context.getBean("HeatQueryService");

        /**
         * 桥梁专项phoenix接口
         */
//
//        for(int i=0;i<100;i++) {
//            BridgeParam bridgeParam = new BridgeParam("340111-04-00-002-000555", "HF_HHDD_NFH_0001", "16_8", "2018-04-01 16:14:47", "2018-04-11 16:14:47");
//            String s = brService.queryDynamic(bridgeParam);
//            System.out.println(s);
//        }
        /**
         * 燃气专项phoenix接口调试
         */
//        GasParam gasParam = new GasParam("000031", "NDIR001710200891", "0_0", "2018-01-30 16:14:47", "2018-03-12 16:14:47");
//        String s = gasQueryService.queryGas(gasParam);
//        System.out.println(s);

        /**
         * 供水专项phoenix接口调试
         */
//        WaterParam waterParam = new WaterParam("00c519", "000223", "1_0", "2017-12-30 16:14:47", "2018-03-12 16:14:47","");
//        String s = waterQueryService.queryWater(waterParam);
//        System.out.println(s);

        /**
         * 排水专项phoenix接口调试000031:1005:2_4:9223370513373108807
         */
//        while (true){
//            DrainParam drainParam = new DrainParam("000031", "1005", "2_4", "2018-02-30 16:14:47", "2018-03-12 16:14:47");
//            String s = drainQueryService.queryDrain(drainParam);
//            System.out.println(s);
//            Thread.sleep(5000l);
//        }

        while(true){
            HeatParam heatParam=new HeatParam("340111-04-00-004-000756", "HF_FHDD_DY000001", "39_7", "2018-05-01 16:14:47", "2018-05-02 16:14:47");
            String s = heatQueryService.queryHeat(heatParam);
            System.out.println(s);
            Thread.sleep(5000l);
        }


        /****************************************/
//        BridgeParam bridgeParam = new BridgeParam("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-11-05 16:14:47", "2017-11-06 16:14:47", "");
//        List<List<Double>> model = brService.queryBridgeModel("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-11-05 16:14:47", "2017-11-06 16:14:47", "");
//        System.out.println(model.size());

        /****************************************/
//        BridgeParam bridgeParam = new BridgeParam("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-11-05 16:14:47", "2017-11-06 16:14:47", "");
//        ArrayList<BridgeParam> objects = new ArrayList<>();
//        objects.add(bridgeParam);
//        List<double[][]> doubles = brService.queryBridgeDouble(objects);
//        for(double[][] d:doubles){
//            for (int i=0;i<d.length;i++){
//                System.out.println(Arrays.toString(d[i]));
//            }
//        }
        /******************** kylin接口 *********************/
//
//        ResultMessage resultMessage = brService.queryBridgeEnviromentV2(new String[]{"1515FHDDKNFHDQDTW1"}, "402884405026354d6000", "20170501", "20171010");
//        System.out.println(resultMessage);
    }
}
