package com.gsafety.bigdata.lifeline.dao;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.log4j.Logger;
import com.gsafety.bigdata.lifeline.model.ResultBean;
import com.gsafety.bigdata.lifeline.util.HBaseUtil;
//import com.gsafety.lifeline.bigdata.avro.Data;

public class HBaseDao {
	private Logger logger = Logger.getLogger(HBaseDao.class);

	/**
	 * 提出公用的数据封装返回数据
	 * @throws ParseException
	 */
	public static ResultBean packData(Iterator<Result> iters,SimpleDateFormat dateFormat2,Result rs) throws ParseException{

		Cell time = rs.getColumnLatestCell("M".getBytes(),"time".getBytes());
		Cell dataType2 = rs.getColumnLatestCell("M".getBytes(),"dataType".getBytes());
		Cell level = rs.getColumnLatestCell("M".getBytes(),"level".getBytes());
		Cell values = rs.getColumnLatestCell("M".getBytes(),"values".getBytes());

		ResultBean bean = new ResultBean();
		bean.setDataType(Bytes.toString(dataType2.getValue()));


		String timeString=Bytes.toString(time.getValue());
		long time2=	dateFormat2.parse(timeString).getTime();
		bean.setTime(time2);

		String valuesString=Bytes.toString(values.getValue());
		String [] stringArr= valuesString.split(",");

		List<Double> doubleArray=new ArrayList<Double>();
		DecimalFormat   df  = new DecimalFormat("#.00");
		for(int i=0;i<stringArr.length;i++){
			Double doublePar = Double.parseDouble(stringArr[i]);
			String doubleValue =df.format(doublePar);
			doubleArray.add(new Double(doubleValue));
		}
		bean.setValues(doubleArray);
		bean.setLevel(Bytes.toInt(level.getValue()));
		return bean;
	}

//	public static ResultBean packDataV2(Iterator<Result> iters,SimpleDateFormat dateFormat2,Result rs) throws ParseException{
//
////		Cell time = rs.getColumnLatestCell("M".getBytes(),"time".getBytes());
////		Cell dataType2 = rs.getColumnLatestCell("M".getBytes(),"dataType".getBytes());
////		Cell level = rs.getColumnLatestCell("M".getBytes(),"level".getBytes());
//		Cell values = rs.getColumnLatestCell("M".getBytes(),"values".getBytes());
//
////		ResultBean bean = new ResultBean();
////		bean.setDataType(Bytes.toString(dataType2.getValue()));
//
//
////		String timeString=Bytes.toString(time.getValue());
////		long time2=	dateFormat2.parse(timeString).getTime();
////		bean.setTime(time2);
//
//		String valuesString=Bytes.toString(values.getValue());
//		String [] stringArr= valuesString.split(",");
//
//		List<Double> doubleArray=new ArrayList<Double>();
//		DecimalFormat   df  = new DecimalFormat("#.00");
//		for(int i=0;i<stringArr.length;i++){
//			Double doublePar = Double.parseDouble(stringArr[i]);
//			String doubleValue =df.format(doublePar);
//			doubleArray.add(new Double(doubleValue));
//		}
//		bean.setValues(doubleArray);
//		bean.setLevel(Bytes.toInt(level.getValue()));
//		return bean;
//	}

	public List<Object> query(String locations, String terninal, String sensor, String starTime, String endTime,
							  String table) {
		Table tb = null;
		ResultScanner res = null;

		List<Object> lists = new ArrayList<Object>();
		Connection connection = null;
		//返回总条数
		int resultTotal = 5000;

		try {
			SimpleDateFormat dateFormat2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			String md5String = sensor + terninal + locations;

			String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			long start = dateFormat.parse(starTime).getTime();
			long end = dateFormat.parse(endTime).getTime();

			String startrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - end)+":00";

			String endrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - start)+"00";

			System.out.println("startrowkey:-------------------"+startrowkey);
			System.out.println("entrowkey:-------------------"+endrowkey);
			logger.info("Startrow :" + startrowkey);
			logger.info("endrow :" + endrowkey);

			TableName tableName = TableName.valueOf(table.toUpperCase());

			logger.info("query  table :" + tableName.getNameAsString());

			connection = HBaseUtil.getConnection();


			long t1 = System.currentTimeMillis();


			tb = connection.getTable(tableName);

			Scan scan = new Scan();

			scan.setStartRow(Bytes.toBytes(startrowkey));
			scan.setStopRow(Bytes.toBytes(endrowkey));
			// scan.setMaxResultSize(10000);
			scan.setCaching(1000);

			res = tb.getScanner(scan);
			Iterator<Result> iters = res.iterator();
			long t2 = System.currentTimeMillis();
			//小于24小时的数据全量返回，大于24小时数据返回最多1万条
			if((end -start) <= 86400000){
				while(iters.hasNext()){
					Result rs = iters.next();
					lists.add(packData(iters, dateFormat2,rs));
				}
				long t3 = System.currentTimeMillis();
				return lists;
			}else{
				long rowCount = 0;
				try {
					System.setProperty("zookeeper.sasl.client", "true");
					AggregationClient aggregationClient = new AggregationClient(connection.getConfiguration());
					//指定扫描列族，唯一值
					scan.addFamily(Bytes.toBytes("M"));
					rowCount = aggregationClient.rowCount(tableName, new LongColumnInterpreter(), scan);
					aggregationClient.close();
				} catch (Throwable e) {
					// TODO Auto-generated catch block
					logger.info("Hbase Count RowKey Exception");
					e.printStackTrace();
				}finally{
					System.setProperty("zookeeper.sasl.client", "false");
				}

				long t3 = System.currentTimeMillis();

				if(rowCount/resultTotal <= 1){

					int count = 0;
					while (iters.hasNext()) {
						Result rs = iters.next();
						lists.add(packData(iters, dateFormat2,rs));
						count++;
					}
					long t4 = System.currentTimeMillis();
					logger.info("count :" + count+"--rowcount:"+rowCount);
					logger.info("seacheTime :" + (t2-t1));
					logger.info("packTime :" + (t4-t3));
					logger.info("countTime :" + (t3-t2));
					logger.info("----returnData----"+lists.size());
					return lists;
				}else{
					int count = 0;
					while (iters.hasNext()) {
						count+=1;
						if(count % (rowCount/resultTotal) == 1){
							Result rs = iters.next();
							lists.add(packData(iters, dateFormat2,rs));
						}else{
							iters.next();
						}
					}
					long t4 = System.currentTimeMillis();
					logger.info("count :" + count+"--rowcount:"+rowCount);
					logger.info("seacheTime :" + (t2-t1));
					logger.info("packTime :" + (t4-t3));
					logger.info("countTime :" + (t3-t2));
					logger.info("----returnData----"+lists.size());
					return lists;
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			HBaseUtil.close(tb, res);
		}
		logger.info("----returnData(end)----"+lists.size());

		return lists;

	}


	public List<List<Double>> queryV2(String locations, String terninal, String sensor, String starTime, String endTime,
							  String table) {
		Table tb = null;
		ResultScanner res = null;

		List<List<Double>> lists = new ArrayList<List<Double>>();
		Connection connection = null;
		//返回总条数
		table = "bridge_" + terninal ;
		try {
			SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			String md5String = sensor + terninal;
			String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			long start = dateFormat.parse(starTime).getTime();
			long end = dateFormat.parse(endTime).getTime();

			String startrowkey = hashcode.substring(0, 6) + ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - end);
			String endrowkey = hashcode.substring(0, 6)+ ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - start);

			logger.info("Startrow :" + startrowkey);
			logger.info("endrow :" + endrowkey);
			System.out.println(startrowkey);
			System.out.println(endrowkey);

			TableName tableName = TableName.valueOf(table.toUpperCase());

			logger.info("query  table :" + tableName.getNameAsString());

			connection = HBaseUtil.getConnection();


			long t1 = System.currentTimeMillis();


			tb = connection.getTable(tableName);
			Scan scan = new Scan();

			scan.setStartRow(Bytes.toBytes(startrowkey));
			scan.setStopRow(Bytes.toBytes(endrowkey));
			// scan.setMaxResultSize(10000);
			scan.setCaching(3000);

			res = tb.getScanner(scan);

			Iterator<Result> iters = res.iterator();

			long t2 = System.currentTimeMillis();
			//小于24小时的数据全量返回，大于24小时数据返回最多1万条
			while(iters.hasNext()){
				Result rs = iters.next();
				lists.add(packData(iters, dateFormat2,rs).getValues());
			}
			long t3 = System.currentTimeMillis();

//			return lists;
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			HBaseUtil.close(tb, res);
		}
		logger.info("----returnData(end)----"+lists.size());
		System.out.println("model size>>>>>>>>"+lists.size());
		return lists;

	}


	public List<Object> queryGas(String locations, String terninal, String sensor, String starTime, String endTime) {

		try {

			String table = "gas_1s";

			return query(locations, terninal, sensor, starTime, endTime, table);

		} catch (Exception e) {
			logger.error("", e);
		}

		return null;

	}

	public List<Object> queryBridge(String locations, String terninal, String sensor, String starTime, String endTime) {

		try {

			String table = "bridge_" + terninal + "_1S";

			return query(locations, terninal, sensor, starTime, endTime, table);

		} catch (Exception e) {
			logger.error("", e);
		}
		return null;
	}
	public List<Object> queryBridgeFeature(String locations, String terninal, String sensor, String starTime, String endTime,String dataType) {

		Table tb = null;
		ResultScanner res = null;

		List<Object> lists = new ArrayList<Object>();
		Connection connection = null;
		try {
			String table = "bridge_" + terninal + "_1S";
			SimpleDateFormat dateFormat2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
			String md5String = sensor + terninal + locations;

			String hashcode = MD5Hash.getMD5AsHex(md5String.getBytes("utf-8"));

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			long start = dateFormat.parse(starTime).getTime();
			long end = dateFormat.parse(endTime).getTime();

			String startrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - end)+":00";

			String endrowkey = hashcode.substring(0, 6) + ":" + locations + ":" + terninal + ":" + sensor + ":"
					+ (Long.MAX_VALUE - start)+":00";

			logger.info("Startrow :" + startrowkey);
			logger.info("endrow :" + endrowkey);
			System.out.println(startrowkey);
			System.out.println(endrowkey);
			TableName tableName = TableName.valueOf(table.toUpperCase());

			logger.info("query  table :" + tableName.getNameAsString());

			connection = HBaseUtil.getConnection();


			long t1 = System.currentTimeMillis();



			tb = connection.getTable(tableName);

			Scan scan = new Scan();


			/*RowFilter rowStart = new RowFilter(CompareOp.GREATER_OR_EQUAL,
			  new BinaryComparator(Bytes.toBytes(startrowkey)));

			  RowFilter rowend = new RowFilter(CompareOp.LESS_OR_EQUAL, new
			  BinaryComparator(Bytes.toBytes(endrowkey)));

			 FilterList filterList = new FilterList(rowStart, rowend);
			scan.setFilter(filterList);*/

			/*Filter valFilter = new ValueFilter(CompareOp.EQUAL, new
					  BinaryComparator(Bytes.toBytes(dataType)));*/
			scan.setStartRow(Bytes.toBytes(startrowkey));
			scan.setStopRow(Bytes.toBytes(endrowkey));
			SingleColumnValueFilter scvf = new SingleColumnValueFilter(
					Bytes.toBytes("M"), Bytes.toBytes("dataType"), CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(dataType)));
			scvf.setFilterIfMissing(true);
			scan.setFilter(scvf);

			RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*:0[7]$"));
			scan.setFilter(rowFilter);

			// scan.setMaxResultSize(10000);
			scan.setCaching(1000);


			long t2 = System.currentTimeMillis();
			res = tb.getScanner(scan);

			logger.info(" query spend time :" + (t2 - t1));
			t1 = System.currentTimeMillis();
			Iterator<Result> iters = res.iterator();
			int count = 0;
			while (iters.hasNext()) {

				Result rs = iters.next();
				lists.add(packData(iters, dateFormat2,rs));
				count++;
			}
			logger.info(" --0.0-- :" + count);
			logger.info(" --LIST-- :" + lists.size());
		} catch (Exception e) {
			logger.error("", e);
		} finally {
			HBaseUtil.close(tb, res);
		}

		return lists;

	}



	public List<Object> queryWater(String locations, String terninal, String sensor, String starTime, String endTime,
								   String sample) {

		try {

			String table = "water_1s";

			return query(locations, terninal, sensor, starTime, endTime, table);

		} catch (Exception e) {
			logger.error("", e);
		}

		return null;

	}

	/**
	 * 查询供水的流量计数据
	 *
	 * @param locations
	 * @param terninal
	 * @param sensor
	 * @param starTime
	 * @param endTime
//	 * @param sample
	 * @return
	 */

	public List<Object> queryWaterFlow(String locations, String terninal, String sensor, String starTime,
									   String endTime) {

		try {

			String table = "WATER_FLOW";

			return query(locations, terninal, sensor, starTime, endTime, table);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;

	}

	public static void main(String[] args) {
		HBaseDao hBaseDao = new HBaseDao();
//		List<Object> query = hBaseDao.queryBridgeFeature("00000000", "HF_JZDL_00000003", "4_4", "2017-01-01 16:00:00", "2017-09-18 16:59:00","PV");
//		System.out.println(query.size()+".........................................");
//		for (Object o:query){
//			System.out.println(o.toString());
//		}
		List<List<Double>> list = hBaseDao.queryV2("340122-04-00-001-000175", "HF_PHDQ_00000001", "1_2", "2017-11-05 16:14:47", "2017-11-06 16:14:47", "");
		System.out.println(list);
	}

}
