//package com.gsafety.bigdata.lifeline.protocol;
//
//import com.gsafety.lifeline.bigdata.avro.PushData;
//import com.gsafety.lifeline.bigdata.util.AvroUtil;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
//import java.util.Collections;
//import java.util.Date;
//import java.util.Properties;
//
///**
// * Created by Administrator on 2017-9-26.
// */
//public class Test {
//	public static void main(String[] args) {
//        ProducerThread producer = new ProducerThread("test",false);
//        producer.start();
//
//		ConsumerThread consumer = new ConsumerThread("test");
//		consumer.start();
//	}
//
//	/**生产者线程*/
//	public static class ProducerThread extends Thread{
//		private Producer<String ,byte[]> producer;
//		private String topic;
//		private boolean isAsyn;
//		private boolean stop = false;
//		private int messageNo = 0;
//		private void init(){
//			Properties props = new Properties();
//			props.put("bootstrap.servers","10.5.4.40:9092");
//			//props.put("client.id","bridge.bigdata.testproducer");
//			if(this.isAsyn) props.put("producer.type","async");
//			//props.put("request.required.acks","1");
//			props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//			props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//			producer = new KafkaProducer<>(props);
//		}
//
//		public ProducerThread(String topic,boolean isAsyn){
//			this.topic = topic;
//			this.isAsyn = isAsyn;
//			init();
//		}
//
//		public void run(){
//			while (!stop){
//				try {
//					messageNo++;
//					Date dt = new Date();
//					long time = dt.getTime();
//					String id = messageNo+"";
//					PushData pd = new PushData("TestCommand",id,time,"这是一条测试命令:"+id,"");
//					byte[] data = AvroUtil.serializer(pd);
//					String key = "TestCommand_"+id+"_"+time;
//					producer.send(new ProducerRecord<String, byte[]>(topic,key,data));
//					System.out.println("发送消息======>>:"+pd.toString());
//					Thread.sleep(5000);
//				}catch (Exception e){
//					System.out.println("发送消息失败:"+e.getMessage());
//				}
//			}
//		}
//	}
//
//	/**消费者线程*/
//	public static class  ConsumerThread extends Thread{
//		private String topic;
//		private boolean stop = false;
//
//
//		private ConsumerThread(String topic){
//			this.topic = topic;
//		}
//
//		private static KafkaConsumer<String, byte[]> kconsumer;
//
//		// Group id
//		private final String groupId = "group.id";
//		// 消息内容使用的反序列化类
//		private final String valueDeserializer = "value.deserializer";
//		// 消息Key值使用的反序列化类
//		private final String keyDeserializer = "key.deserializer";
//		// 自动提交offset的时间间隔
//		private final String autoCommitIntervalMs = "auto.commit.interval.ms";
//		// 会话超时时间
//		private final String sessionTimeoutMs = "session.timeout.ms";
//
//		/**
//		 * 初始化消费者实例
//		 */
//		public void init(){
//			Properties props = new Properties();
//			props.put("bootstrap.servers","10.5.4.40:9092");
//			//props.put("client.id","bridge.bigdata.testconsumer");
//			//props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//			//props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//			props.put(groupId, "bridge.bigdata.test");
//			// 自动提交offset的时间间隔
//			props.put(autoCommitIntervalMs, "1000");
//			// 会话超时时间
//			props.put(sessionTimeoutMs, "30000");
//			// 消息Key值使用的反序列化类
//			props.put(keyDeserializer, "org.apache.kafka.common.serialization.StringDeserializer");
//			// 消息内容使用的反序列化类
//			props.put(valueDeserializer, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//			kconsumer = new KafkaConsumer<String, byte[]>(props);//根据配置生成消费者实例
//		}
//
//		public void run(){
//			init();
//			kconsumer.subscribe(Collections.singletonList(topic));//根据topic进行订阅
//			while (!stop) {
//				try {
//					ConsumerRecords<String, byte[]> records = kconsumer.poll(100);//每100ms获取一次记录集合
//					for (ConsumerRecord<String, byte[]> record : records) {//遍历结果集
//						byte[] bytes = record.value();
//						if (bytes != null) {//不为空的情况下 进行反序列化 再进一步进行业务处理
//							PushData data = AvroUtil.deserializePushData(bytes);
//							System.out.println("<<<<<<====收到消息"+data.toString());
//						}
//					}
//				}catch (Exception e){
//					System.out.println("接收消息失败:"+e.getMessage());
//				}
//			}
//		}
//	}
//
//}
