package com.gsafety.bigdata.lifeline.util;

import com.gsafety.bigdata.lifeline.pojo.ExportParam;
import com.gsafety.bigdata.lifeline.service.impl.ExportQueryServiceImpl;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: yifeng G
 * @Date: Create in 10:05 2017/12/12 2017
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class TestExport implements Runnable {

    public static void main(String[] args) {
        TestExport export = new TestExport();
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.execute(export);
    }

    @Override
    public void run() {
        ExportQueryServiceImpl exportQueryService = new ExportQueryServiceImpl();
//        ExportParam param = new ExportParam("340122-04-00-012-000694",
//                "HF_G206_00000001", "37_11", "2017-12-29", "2017-12-30", "bridge",
//                "LOWSUMRY","taskNo","running");
//        ExportParam param = new ExportParam("",
//                "NDIR001711251211", "0_0", "2018-03-04", "2018-03-14", "gas",
//                "","taskNo","running");
//        ExportParam param = new ExportParam("",
//                "", "", "", "", "gas",
//                "","taskNo","running");
        ExportParam param = new ExportParam("340122-04-00-001-000183",
                "HF_JZHS_00000001", "1_5", "2018-02-01", "2018-02-01", "bridge",
                "ACCE","taskNo","running");
//        ExportParam param = new ExportParam("340111-04-00-004-000733",
//                "HF_FHDD_DY000001", "38_5", "2018-03-08", "2018-03-08", "bridge",
//                "LOWSUMRY","","running");
//        ExportParam param = new ExportParam("340121-06-00-021-000505",
//                "NDIR001710090771", "", "2018-01-03", "2018-01-05", "gas",
//                "methane","taskNo","running");
        try {
//            System.out.println(exportQueryService.queryData(param));
            System.out.println(exportQueryService.queryData(param));
//            System.out.println(exportQueryService.queryData(param).get("zipPATH"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
