package com.gsafety.bigdata.lifeline.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBaseUtil {

	private static Logger logger = Logger.getLogger(HBaseUtil.class);
	private static Configuration conf = null;
	private static Connection connection = getConnection();

	public static void close(Table tb, ResultScanner res) {

		if (tb != null) {
			try {
				tb.close();
			} catch (IOException e) {
				logger.error("", e);
			}
		}

		if (res != null) {
			res.close();
		}
	}

	public static Connection getConnection()  {
		if(connection!=null)
			return connection;
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
			System.out.println(connection.getAdmin().getClusterStatus());
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
		return connection;
	}

	public static void main(String[] args) {

		/*String table = "BRIDGE_HF_FHDD_DY000001_S";
		String location = "00000000";
		String terninal = "HF_FHDD_DY000001";
		String sensor = "40_2";
		String startTime = "2017-05-23 12:50:00";
		String endTime = "2017-05-29 14:51:00";

		List<Object> lists = query(location, terninal, sensor, startTime, endTime, table);

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		for (Object obj : lists) {

			ResultBean bean = (ResultBean) obj;
			System.out.println(dateFormat.format(new Date(bean.getTime())));
			System.out.println(bean.getValues());
		}*/
		try {
			Connection connection = getConnection();
//			Admin admin = connection.getAdmin();
//			TableName[] tableNames = admin.listTableNames();
//			for(TableName tableName:tableNames){
//				System.out.println(tableName.getNameAsString());
//			}
			Table table = connection.getTable(TableName.valueOf("BRIDGE_HF_JZDL_00000003_1S"));
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes("5fa795:00000000:HF_JZDL_00000003:4_4:9223370532166435807:00"));
			scan.setStopRow(Bytes.toBytes("5fa795:00000000:HF_JZDL_00000003:4_4:9223370553165175807:00"));
			ResultScanner scanner = table.getScanner(scan);
			for(Result result:scanner){
				System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("M"),Bytes.toBytes("dataType"))));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}
